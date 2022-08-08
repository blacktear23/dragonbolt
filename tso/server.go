package tso

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/blacktear23/bolt"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/config"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

var (
	defaultTSOKey = []byte("tso_default")

	ErrInvalidTSO = errors.New("Invalid TSO")
)

type TSOServer struct {
	shardID   uint64
	replicaID uint64
	timeout   time.Duration
	nh        *dragonboat.NodeHost
	db        *bolt.DB
	cs        *client.Session
}

func NewTSOServer(nh *dragonboat.NodeHost, shardID uint64, replicaID uint64, dbFile string, initMembers map[uint64]string) (*TSOServer, error) {
	cfg := config.Config{
		ReplicaID:          replicaID,
		ShardID:            shardID,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    0,
		CompactionOverhead: 16,
	}
	tsoDBFile := fmt.Sprintf("%s.tso.db", dbFile)
	db, err := bolt.Open(tsoDBFile, 0600, nil)
	if err != nil {
		return nil, err
	}
	builder := TSOKVBuilder{
		DB: db,
	}
	err = nh.StartOnDiskReplica(initMembers, false, builder.Build, cfg)
	if err != nil {
		return nil, err
	}
	cs := nh.GetNoOPSession(shardID)
	return &TSOServer{
		shardID:   shardID,
		replicaID: replicaID,
		timeout:   10 * time.Second,
		nh:        nh,
		db:        db,
		cs:        cs,
	}, nil
}

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func (s *TSOServer) trySyncPropose(data []byte, tryTimes int) (sm.Result, error) {
	var (
		result sm.Result
		err    error
	)
	for i := 0; i < tryTimes; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
		result, err = s.nh.SyncPropose(ctx, s.cs, data)
		cancel()
		if err == nil {
			return result, nil
		}
		// Err is not nil
		if err == dragonboat.ErrShardNotReady {
			dur := min(1<<i, 500)
			time.Sleep(time.Duration(dur) * time.Millisecond)
			continue
		} else {
			return result, err
		}
	}
	return result, err
}

func (s *TSOServer) GetTSO() (uint64, error) {
	req := tsoRequest{
		Key: defaultTSOKey,
	}
	data, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}
	ret, err := s.trySyncPropose(data, 1000)
	if err != nil {
		return 0, err
	}
	if ret.Value == 0 {
		log.Printf("%+v", ret)
		return 0, ErrInvalidTSO
	}
	return ret.Value, nil
}

type tsoRequest struct {
	Key []byte `json:key`
}
