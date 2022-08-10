package txn

import (
	"errors"
)

var (
	_ Txn = (Txn)(&RcTxn{})

	ErrNotBegin = errors.New("Transaction not begin")
)

type RcTxn struct {
	ops   KVOperation
	memdb *MemDB
}

func NewRcTxn(kvOps KVOperation) *RcTxn {
	return &RcTxn{
		ops: kvOps,
	}
}

func (t *RcTxn) Begin() error {
	t.memdb = NewMemDB()
	return nil
}

func (t *RcTxn) Commit() error {
	if t.memdb == nil {
		return ErrNotBegin
	}
	muts := t.memdb.GetMutations()
	err := t.ops.Batch(muts)
	t.memdb = nil
	return err
}

func (t *RcTxn) Rollback() error {
	// Just drop memdb
	t.memdb = nil
	return nil
}

func (t *RcTxn) Set(key []byte, value []byte) error {
	if t.memdb == nil {
		return ErrNotBegin
	}
	t.memdb.Set(key, value)
	return nil
}

func (t *RcTxn) Delete(key []byte) error {
	if t.memdb == nil {
		return ErrNotBegin
	}
	t.memdb.Delete(key)
	return nil
}

func (t *RcTxn) Get(key []byte) ([]byte, error) {
	if t.memdb == nil {
		return nil, ErrNotBegin
	}
	ret, have := t.memdb.Get(key)
	if have {
		return ret, nil
	}
	return t.ops.Get(key)
}

func (t *RcTxn) Cursor() (Cursor, error) {
	if t.memdb == nil {
		return nil, ErrNotBegin
	}
	return &RcTxnCursor{
		memdb: t.memdb,
		ops:   t.ops,
	}, nil
}

type RcTxnCursor struct {
	memdb *MemDB
	ops   KVOperation
}

func (c *RcTxnCursor) Seek(key []byte) error {
	return nil
}

func (c *RcTxnCursor) Next() (key []byte, value []byte, err error) {
	return
}
