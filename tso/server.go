package tso

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path"
	"time"

	"github.com/blacktear23/bolt"
	"github.com/blacktear23/dragonbolt/store"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/config"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

const (
	OP_TSO       = 1
	OP_CREATE_DB = 10
	OP_UPDATE_DB = 11
	OP_DELETE_DB = 12
	OP_UP_DB     = 13
	OP_DOWN_DB   = 14
	OP_QUERY_DBS = 20
)

var (
	defaultTSOKey = []byte("tso_default")
	defaultIDKey  = []byte("id_default")

	ErrInvalidTSO     = errors.New("Invalid TSO")
	ErrInvalidID      = errors.New("Invalid ID")
	ErrCannotCreateDB = errors.New("Cannot create db")
	ErrCannotDeleteDB = errors.New("Cannot delete db")
	ErrCannotUpDB     = errors.New("Cannot up db")
	ErrCannotDownDB   = errors.New("Cannot down db")
)

type DBInfo struct {
	Name    string
	ShardID uint64
}

type tsoRequest struct {
	Op      int    `json:"op"`
	Key     []byte `json:"key"`
	Name    string `json:"name"`
	ShardID uint64 `json:"shard_id"`
}

type tsoQuery struct {
	Op int
}

type tsoQueryResult struct {
	DBInfoList []*DBInfo
}

type TSOServer struct {
	shardID      uint64
	replicaID    uint64
	startShardID uint64
	timeout      time.Duration
	nh           *dragonboat.NodeHost
	sm           *store.StoreManager
	db           *bolt.DB
	cs           *client.Session
}

func NewTSOServer(nh *dragonboat.NodeHost, shardID uint64, replicaID uint64, dbDir string, initMembers map[uint64]string, startShardID uint64, sm *store.StoreManager) (*TSOServer, error) {
	cfg := config.Config{
		ReplicaID:          replicaID,
		ShardID:            shardID,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    0,
		CompactionOverhead: 16,
	}
	tsoDBFile := path.Join(dbDir, fmt.Sprintf("tso-%d.db", replicaID))
	db, err := bolt.Open(tsoDBFile, 0600, nil)
	if err != nil {
		return nil, err
	}
	builder := TSOKVBuilder{
		NodeHost:     nh,
		StoreManager: sm,
		DB:           db,
		ReplicaID:    replicaID,
		InitMembers:  initMembers,
	}
	err = nh.StartOnDiskReplica(initMembers, false, builder.Build, cfg)
	if err != nil {
		return nil, err
	}
	cs := nh.GetNoOPSession(shardID)
	return &TSOServer{
		shardID:      shardID,
		replicaID:    replicaID,
		timeout:      10 * time.Second,
		nh:           nh,
		db:           db,
		cs:           cs,
		startShardID: startShardID,
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

func (s *TSOServer) trySyncRead(query *tsoQuery, tryTimes int) (*tsoQueryResult, error) {
	var (
		result interface{}
		err    error
	)
	for i := 0; i < tryTimes; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
		result, err = s.nh.SyncRead(ctx, s.shardID, query)
		cancel()
		if err == nil {
			return result.(*tsoQueryResult), nil
		}
		// Err is not nil
		if err == dragonboat.ErrShardNotReady {
			dur := min(1<<i, 500)
			time.Sleep(time.Duration(dur) * time.Millisecond)
			continue
		} else {
			return nil, err
		}
	}
	if result == nil {
		return nil, err
	}
	return result.(*tsoQueryResult), err
}

func (s *TSOServer) GetTSO() (uint64, error) {
	req := tsoRequest{
		Op:  OP_TSO,
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

func (s *TSOServer) GetUniqID() (uint64, error) {
	req := tsoRequest{
		Op:  OP_TSO,
		Key: defaultIDKey,
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
		return 0, ErrInvalidID
	}
	return ret.Value, nil
}

func (s *TSOServer) CreateDB(name string) (*DBInfo, error) {
	nid, err := s.GetUniqID()
	if err != nil {
		return nil, err
	}
	sid := nid + s.startShardID
	req := tsoRequest{
		Op:      OP_CREATE_DB,
		Name:    name,
		ShardID: sid,
	}
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	ret, err := s.trySyncPropose(data, 1000)
	if err != nil {
		return nil, err
	}
	if ret.Value == 0 {
		return nil, ErrCannotCreateDB
	}
	return &DBInfo{
		Name:    name,
		ShardID: sid,
	}, nil
}

func (s *TSOServer) UpDB(info *DBInfo) error {
	req := tsoRequest{
		Op:      OP_UP_DB,
		Name:    info.Name,
		ShardID: info.ShardID,
	}
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	ret, err := s.trySyncPropose(data, 1000)
	if err != nil {
		return err
	}
	if ret.Value == 0 {
		return ErrCannotUpDB
	}
	return nil
}

func (s *TSOServer) DownDB(info *DBInfo) error {
	req := tsoRequest{
		Op:      OP_DOWN_DB,
		Name:    info.Name,
		ShardID: info.ShardID,
	}
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	ret, err := s.trySyncPropose(data, 1000)
	if err != nil {
		return err
	}
	if ret.Value == 0 {
		return ErrCannotDownDB
	}
	return nil
}

func (s *TSOServer) DeleteDB(name string) error {
	req := tsoRequest{
		Op:   OP_DELETE_DB,
		Name: name,
	}
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	ret, err := s.trySyncPropose(data, 1000)
	if err != nil {
		return err
	}
	if ret.Value == 0 {
		return ErrCannotDeleteDB
	}
	return nil
}

func (s *TSOServer) ListDB() ([]*DBInfo, error) {
	query := &tsoQuery{
		Op: OP_QUERY_DBS,
	}
	result, err := s.trySyncRead(query, 1000)
	if err != nil {
		return nil, err
	}
	return result.DBInfoList, nil
}
