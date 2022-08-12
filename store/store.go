package store

import (
	"fmt"
	"os"
	"path"

	"github.com/blacktear23/bolt"
	"github.com/blacktear23/dragonbolt/kv"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

type BoltDBStore struct {
	Dir      string
	FileName string
	StoreID  uint64
	DB       *bolt.DB
}

func NewBoltDBStore(dbDir string, storeID uint64) (*BoltDBStore, error) {
	fname := path.Join(dbDir, fmt.Sprintf("data-%d.db", storeID))
	db, err := bolt.Open(fname, 0600, nil)
	if err != nil {
		return nil, err
	}
	return &BoltDBStore{
		Dir:      dbDir,
		FileName: fname,
		StoreID:  storeID,
		DB:       db,
	}, nil
}

func (s *BoltDBStore) Build(clusterID uint64, nodeID uint64) sm.IOnDiskStateMachine {
	return kv.NewDiskKV(clusterID, nodeID, s.DB)
}

func (s *BoltDBStore) Close() error {
	return s.DB.Close()
}

func (s *BoltDBStore) Delete() error {
	return os.Remove(s.FileName)
}

func (s *BoltDBStore) StartReplica(nh *dragonboat.NodeHost, members map[uint64]string, replicaID uint64, join bool) error {
	cfg := config.Config{
		ReplicaID:          replicaID,
		ShardID:            s.StoreID,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    0,
		CompactionOverhead: 16,
	}
	return nh.StartOnDiskReplica(members, join, s.Build, cfg)
}
