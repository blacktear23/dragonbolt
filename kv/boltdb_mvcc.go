package kv

import (
	"github.com/blacktear23/bolt"
	"github.com/blacktear23/dragonbolt/mvcc"
)

func (d *DiskKV) processMvccGet(query *Query) (*QueryResult, error) {
	ret := &QueryResult{}
	err := d.db.View(func(txn *bolt.Tx) error {
		mvccTxn := mvcc.NewTwoCFMvcc(txn)
		for _, key := range query.Keys {
			val, err := mvccTxn.Get(query.Version, key)
			if err != nil && !mvcc.IsKeyNotFoundError(err) {
				return err
			}
			ret.KVS = append(ret.KVS, buildKVP(key, val, query.Op))
		}
		return nil
	})
	if err == nil && d.closed {
		panic("lookup returned valid result when DiskKV is already closed")
	}
	return ret, err
}

func (d *DiskKV) processMvccScan(query *Query) (*QueryResult, error) {
	ret := &QueryResult{}
	err := d.db.View(func(txn *bolt.Tx) error {
		mvccTxn := mvcc.NewTwoCFMvcc(txn)
		c := mvccTxn.Cursor(query.Version)
		// Check for first result
		k, v := c.Seek(query.Start)
		// CHeck for rest results
		for i := 0; i < query.Limit; i++ {
			if k == nil {
				// Nothing
				return nil
			} else if keyCompare(k, query.End) >= 0 {
				// Next greater or equals than end key just return nothing
				return nil
			}
			ret.AddKVPair(k, v, query.Op)
			k, v = c.Next()
		}
		return nil
	})
	if err == nil && d.closed {
		panic("lookup returned valid result when DiskKV is already closed")
	}
	return ret, err
}

func (d *DiskKV) processMvccMutation(txn *bolt.Tx, mut Mutation) (uint64, error) {
	var (
		err error
	)
	mvccTxn := mvcc.NewTwoCFMvcc(txn)
	switch mut.Op {
	case MVCC_SET:
		err = mvccTxn.Set(mut.Version, mut.CommitVersion, mut.Key, mut.Value)
	case MVCC_DEL:
		err = mvccTxn.Delete(mut.Version, mut.CommitVersion, mut.Key)
	case MVCC_LOCK:
		err = mvccTxn.LockKey(mut.Version, mut.Key)
	case MVCC_UNLOCK:
		err = mvccTxn.UnlockKey(mut.Version, mut.Key, false)
	case MVCC_UNLOCK_FORCE:
		err = mvccTxn.UnlockKey(mut.Version, mut.Key, true)
	}
	if err != nil {
		if mvcc.IsKeyLockedError(err) {
			return RESULT_KEY_LOCKED, err
		}
		if mvcc.IsTxnConflictError(err) {
			return RESULT_TXN_CONFLICT, err
		}
		return RESULT_ERR, err
	}
	return RESULT_OK, nil
}
