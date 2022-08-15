package kv

import (
	"errors"
	"time"

	"github.com/blacktear23/bolt"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

const (
	appliedIndexKey    string = "disk_kv_applied_index"
	testDBDirName      string = "example-data"
	currentDBFilename  string = "current"
	updatingDBFilename string = "current.updating"
	bucketName         string = "data"
)

var (
	ErrBucketNotExists       = errors.New("Bucket not exists")
	ErrInvalidQuery          = errors.New("Invalid Query")
	ErrUnkonwnQueryOperation = errors.New("Unknown query operation")
	ErrInvalidColumnFamily   = errors.New("Invalid Column Family")

	tsoSrv       TSOSrv = nil
	errTsoNotSet        = errors.New("TSO Service not setted")
	gcDuration   int    = 600
)

type TSOSrv interface {
	GetTSO() (uint64, error)
}

type DiskKVBuilder struct {
	DB *bolt.DB
}

func (b *DiskKVBuilder) Build(clusterID uint64, nodeID uint64) sm.IOnDiskStateMachine {
	return &DiskKV{
		clusterID: clusterID,
		nodeID:    nodeID,
		db:        b.DB,
	}
}

func SetTsoService(srv TSOSrv) {
	tsoSrv = srv
}

func SetGCDuration(i int) {
	gcDuration = i
}

func getTso() (uint64, error) {
	if tsoSrv == nil {
		return 0, errTsoNotSet
	}
	return tsoSrv.GetTSO()
}

func getGCDuration() time.Duration {
	return time.Duration(gcDuration) * time.Second
}
