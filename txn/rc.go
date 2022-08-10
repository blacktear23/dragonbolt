package txn

import (
	"bytes"
	"errors"

	"github.com/blacktear23/dragonbolt/kv"
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
		memdb:     t.memdb,
		memIter:   t.memdb.Iter(),
		ops:       t.ops,
		batchSize: 50,
	}, nil
}

type RcTxnCursor struct {
	memdb     *MemDB
	memIter   Iter
	ops       KVOperation
	batchSize int

	// KV related fields
	kvbuf []kv.KVPair
	kvidx int
	kvcur *kv.KVPair

	// MemDB related fields
	memcur *kv.KVPair
}

func (c *RcTxnCursor) kvSeek(key []byte) error {
	var err error
	c.kvbuf, err = c.ops.Scan(key, nil, c.batchSize)
	if err != nil {
		return err
	}
	if len(c.kvbuf) > 0 {
		c.kvcur = &c.kvbuf[0]
		c.kvidx = 0
		if c.memdb.IsDelete(c.kvcur.Key) {
			c.nextKV()
		}
	} else {
		c.kvcur = nil
		c.kvidx = 0
	}
	return nil
}

func (c *RcTxnCursor) memSeek(key []byte) {
	memsk, memsv := c.memIter.Seek(key)
	if memsk == nil {
		c.memcur = nil
	} else {
		c.memcur = &kv.KVPair{Key: memsk, Value: memsv}
	}
}

func (c *RcTxnCursor) Seek(key []byte) error {
	err := c.kvSeek(key)
	if err != nil {
		return err
	}
	c.memSeek(key)
	return nil
}

func (c *RcTxnCursor) nextKV() error {
	var err error
	for {
		if c.kvcur == nil {
			return nil
		}
		c.kvidx += 1
		if c.kvidx >= len(c.kvbuf) {
			lastKey := c.kvcur.Key
			c.kvbuf, err = c.ops.Scan(lastKey, nil, c.batchSize)
			if err != nil {
				// Error means no kv current
				c.kvcur = nil
				return err
			}
			if len(c.kvbuf) <= 1 {
				// Empty means no kv current
				c.kvcur = nil
				return nil
			}
			// Refill buffer use last available key
			// just skip first key-value because it already iterated
			c.kvcur = nil
			c.kvidx = 1
		}
		c.kvcur = &c.kvbuf[c.kvidx]
		if !c.memdb.IsDelete(c.kvcur.Key) {
			// Not delete means it current can be used
			return nil
		}
	}
}

func (c *RcTxnCursor) nextMem() {
	if c.memcur == nil {
		return
	}
	key, val := c.memIter.Next()
	if key == nil {
		c.memcur = nil
	} else {
		c.memcur = &kv.KVPair{Key: key, Value: val}
	}
}

func (c *RcTxnCursor) Next() (skey []byte, sval []byte, err error) {
	if c.memcur == nil && c.kvcur == nil {
		return
	} else if c.memcur == nil {
		skey = c.kvcur.Key
		sval = c.kvcur.Value
		err = c.nextKV()
		return
	} else if c.kvcur == nil {
		skey = c.memcur.Key
		sval = c.memcur.Value
		c.nextMem()
		return
	}
	cmp := c.compare(c.memcur, c.kvcur)
	if cmp < 0 {
		// mem key less than kv key just return current mem and move mem next
		skey = c.memcur.Key
		sval = c.memcur.Value
		c.nextMem()
		return
	} else if cmp == 0 {
		// Equals just return mem and move both
		skey = c.memcur.Key
		sval = c.memcur.Value
		c.nextMem()
		err = c.nextKV()
	} else {
		// kv key less than mem key just return current kv and move kv next
		skey = c.kvcur.Key
		sval = c.kvcur.Value
		err = c.nextKV()
		return
	}
	return
}

func (c *RcTxnCursor) compare(l, r *kv.KVPair) int {
	if r.Key == nil {
		return -1
	}
	return bytes.Compare(l.Key, r.Key)
}
