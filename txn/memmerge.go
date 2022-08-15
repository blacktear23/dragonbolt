package txn

import (
	"bytes"
	"errors"

	"github.com/blacktear23/dragonbolt/kv"
)

var (
	_ Txn    = (*MemMergeTxn)(nil)
	_ Cursor = (*memMergeTxnCursor)(nil)

	ErrNotBegin = errors.New("Transaction not begin")
)

type MemMergeTxn struct {
	putOp      int
	delOp      int
	ver        uint64
	ops        KVOperation
	memdb      *MemDB
	lockedKeys map[string]bool
}

func NewMemMergeTxn(kvOps KVOperation, putOp int, delOp int, ver uint64) *MemMergeTxn {
	return &MemMergeTxn{
		putOp:      putOp,
		delOp:      delOp,
		ver:        ver,
		ops:        kvOps,
		lockedKeys: make(map[string]bool),
	}
}

func (t *MemMergeTxn) Begin() error {
	t.memdb = NewMemDB(t.putOp, t.delOp, t.ver)
	return nil
}

func (t *MemMergeTxn) Commit(commitVer uint64) error {
	if t.memdb == nil {
		return ErrNotBegin
	}
	muts := t.memdb.GetMutations(commitVer)
	err := t.ops.Batch(muts)
	if len(t.lockedKeys) > 0 {
		t.cleanKeys()
	}
	t.memdb = nil
	return err
}

func (t *MemMergeTxn) LockKey(key []byte) error {
	err := t.lockKey(key)
	if err != nil {
		return err
	}
	t.lockedKeys[string(key)] = true
	return nil
}

func (t *MemMergeTxn) UnlockKey(key []byte) error {
	err := t.unlockKey(key)
	if err != nil {
		return err
	}
	delete(t.lockedKeys, string(key))
	return nil
}

func (t *MemMergeTxn) lockKey(key []byte) error {
	return t.ops.LockKey(key)
}

func (t *MemMergeTxn) unlockKey(key []byte) error {
	return t.ops.UnlockKey(key)
}

func (t *MemMergeTxn) cleanKeys() {
	for skey, _ := range t.lockedKeys {
		t.unlockKey([]byte(skey))
	}
}

func (t *MemMergeTxn) Rollback() error {
	// Just drop memdb
	if len(t.lockedKeys) > 0 {
		t.cleanKeys()
	}
	t.memdb = nil
	return nil
}

func (t *MemMergeTxn) Set(key []byte, value []byte) error {
	if t.memdb == nil {
		return ErrNotBegin
	}
	t.memdb.Set(key, value)
	return nil
}

func (t *MemMergeTxn) BatchSet(kvs []kv.KVPair) error {
	if t.memdb == nil {
		return ErrNotBegin
	}
	for _, kv := range kvs {
		t.memdb.Set(kv.Key, kv.Value)
	}
	return nil
}

func (t *MemMergeTxn) Delete(key []byte) error {
	if t.memdb == nil {
		return ErrNotBegin
	}
	t.memdb.Delete(key)
	return nil
}

func (t *MemMergeTxn) Get(key []byte) ([]byte, error) {
	if t.memdb == nil {
		return nil, ErrNotBegin
	}
	ret, have := t.memdb.Get(key)
	if have {
		return ret, nil
	}
	return t.ops.Get(key)
}

func (t *MemMergeTxn) BatchGet(keys [][]byte) ([]kv.KVPair, error) {
	if t.memdb == nil {
		return nil, ErrNotBegin
	}
	ret := make([]kv.KVPair, len(keys))
	emptyIds := make([]int, 0, len(keys))
	emptyKeys := make([][]byte, 0, len(keys))
	for i, key := range keys {
		mval, have := t.memdb.Get(key)
		if have {
			ret[i] = kv.KVPair{Key: key, Value: mval}
		} else {
			emptyIds = append(emptyIds, i)
			emptyKeys = append(emptyKeys, key)
		}
	}
	if len(emptyIds) == 0 {
		return ret, nil
	}
	kvs, err := t.ops.BatchGet(emptyKeys)
	if err != nil {
		return nil, err
	}
	for i, kv := range kvs {
		idx := emptyIds[i]
		ret[idx] = kv
	}
	return ret, nil
}

func (t *MemMergeTxn) Cursor() (Cursor, error) {
	if t.memdb == nil {
		return nil, ErrNotBegin
	}
	return &memMergeTxnCursor{
		memdb:     t.memdb,
		memIter:   t.memdb.Iter(),
		ops:       t.ops,
		batchSize: 50,
	}, nil
}

type memMergeTxnCursor struct {
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

func (c *memMergeTxnCursor) kvSeek(key []byte) error {
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

func (c *memMergeTxnCursor) memSeek(key []byte) {
	memsk, memsv := c.memIter.Seek(key)
	if memsk == nil {
		c.memcur = nil
	} else {
		c.memcur = &kv.KVPair{Key: memsk, Value: memsv}
	}
}

func (c *memMergeTxnCursor) Seek(key []byte) error {
	err := c.kvSeek(key)
	if err != nil {
		return err
	}
	c.memSeek(key)
	return nil
}

func (c *memMergeTxnCursor) nextKV() error {
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

func (c *memMergeTxnCursor) nextMem() {
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

func (c *memMergeTxnCursor) Next() (skey []byte, sval []byte, err error) {
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

func (c *memMergeTxnCursor) compare(l, r *kv.KVPair) int {
	if r.Key == nil {
		return -1
	}
	return bytes.Compare(l.Key, r.Key)
}
