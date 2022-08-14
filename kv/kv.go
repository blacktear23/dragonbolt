package kv

import (
	"errors"

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
)

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
