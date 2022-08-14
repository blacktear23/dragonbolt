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
	BatchGet(keys [][]byte) ([]kv.KVPair, error)
	Scan(start []byte, end []byte, limit int) ([]kv.KVPair, error)
	LockKey(key []byte) error
	UnlockKey(key []byte) error
}

type Cursor interface {
	Seek(key []byte) error
	Next() ([]byte, []byte, error)
}

type Txn interface {
	Begin() error
	Commit(ver uint64) error
	Rollback() error
	Set(key []byte, value []byte) error
	Get(key []byte) (value []byte, err error)
	BatchGet(keys [][]byte) ([]kv.KVPair, error)
	BatchSet(kvs []kv.KVPair) error
	Delete(key []byte) error
	Cursor() (Cursor, error)
	LockKey(key []byte) error
	UnlockKey(key []byte) error
}
