package txn

import "github.com/blacktear23/dragonbolt/kv"

const UINT64_MAX uint64 = 0xFFFFFFFFFFFFFFFF

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

type KVCfOperation interface {
	GetTSO() (uint64, error)
	BatchSet(muts []kv.Mutation) error
	Set(cf string, kvs []kv.KVPair) error
	Get(cf string, key []byte) ([]byte, error)
	BatchGet(cf string, keys [][]byte) ([][]byte, error)
	Scan(cf string, start []byte, end []byte, limit int) ([]kv.KVPair, error)
}

type KVOperation interface {
	Batch(muts []kv.Mutation) error
	Get(key []byte) ([]byte, error)
	Scan(start []byte, end []byte, limit int) ([]kv.KVPair, error)
}

type Cursor interface {
	Seek(key []byte) error
	Next() (key []byte, value []byte, err error)
}

type Txn interface {
	Begin() error
	Commit() error
	Rollback() error
	Set(key []byte, value []byte) error
	Get(key []byte) (value []byte, err error)
	Delete(key []byte) error
	Cursor() (Cursor, error)
}
