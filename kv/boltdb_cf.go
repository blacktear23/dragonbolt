package kv

import (
	"github.com/blacktear23/bolt"
	"github.com/blacktear23/dragonbolt/mvcc"
)

func sameLen(val1 []byte, val2 []byte, sameLen bool) bool {
	if sameLen {
		return len(val1) == len(val2)
	}
	return true
}

func (d *DiskKV) checkColumnFamily(cf string) ([]byte, error) {
	switch cf {
	case CFData, CFLock, CFWrite, mvcc.CFKeys, mvcc.CFValues:
		return []byte(cf), nil
	default:
		return nil, ErrInvalidColumnFamily
	}
}

func (d *DiskKV) processCfGet(query *Query) (*QueryResult, error) {
	ret := &QueryResult{}
	cfName, err := d.checkColumnFamily(query.Cf)
	if err != nil {
		return ret, err
	}
	err = d.db.View(func(txn *bolt.Tx) error {
		cfBucket := txn.Bucket(cfName)
		for _, key := range query.Keys {
			var val []byte = nil
			if cfBucket != nil {
				val = cfBucket.Get(key)
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

func (d *DiskKV) processCfScan(query *Query) (*QueryResult, error) {
	// Scan for [Start, End)
	ret := &QueryResult{}
	cfName, err := d.checkColumnFamily(query.Cf)
	if err != nil {
		return ret, err
	}
	err = d.db.View(func(txn *bolt.Tx) error {
		cfBucket := txn.Bucket(cfName)
		if cfBucket == nil {
			// cf not exists just return empty list
			return nil
		}
		c := cfBucket.Cursor()
		// Check for first result
		k, v := c.Seek(query.Start)
		if k == nil {
			// Nothing
			return nil
		} else if keyCompare(k, query.End) >= 0 {
			// Next greater or equals than end key just return nothing
			return nil
		}
		i := 0
		if sameLen(k, query.End, query.SameLen) {
			ret.AddKVPair(k, v, query.Op)
			i++
		}

		// Check for rest results
		for i < query.Limit {
			k, v = c.Next()
			if k == nil {
				// Nothing
				return nil
			} else if keyCompare(k, query.End) >= 0 {
				// Next greater or equals than end key just return nothing
				return nil
			}
			if sameLen(k, query.End, query.SameLen) {
				ret.AddKVPair(k, v, query.Op)
				i++
			}
		}
		return nil
	})
	if err == nil && d.closed {
		panic("lookup returned valid result when DiskKV is already closed")
	}
	return ret, err
}

func (d *DiskKV) processCfReverseScan(query *Query) (*QueryResult, error) {
	// Scan for [Start, End]
	ret := &QueryResult{}
	cfName, err := d.checkColumnFamily(query.Cf)
	if err != nil {
		return ret, err
	}
	err = d.db.View(func(txn *bolt.Tx) error {
		cfBucket := txn.Bucket(cfName)
		if cfBucket == nil {
			// cf not exists just return empty list
			return nil
		}
		c := cfBucket.Cursor()
		i := 0
		// Check for last result
		k, v := c.Seek(query.End)
		if k == nil {
			// Do nothing
		} else if keyCompare(k, query.End) <= 0 {
			if sameLen(k, query.End, query.SameLen) {
				ret.AddKVPair(k, v, query.Op)
				i++
			}
		}
		for i < query.Limit {
			k, v = c.Prev()
			if k == nil {
				// Nothing
				return nil
			} else if keyCompare(k, query.Start) < 0 {
				return nil
			}
			if sameLen(k, query.Start, query.SameLen) {
				ret.AddKVPair(k, v, query.Op)
				i++
			}
		}
		return nil
	})
	if err == nil && d.closed {
		panic("lookup returned valid result when DiskKV is already closed")
	}
	return ret, err
}

func (d *DiskKV) processCFMutation(txn *bolt.Tx, mut Mutation) (uint64, error) {
	var (
		err      error
		cfbucket *bolt.Bucket
		cfName   []byte
	)
	cfName, err = d.checkColumnFamily(mut.Cf)
	if err != nil {
		return RESULT_ERR, err
	}
	cfbucket, err = txn.CreateBucketIfNotExists(cfName)
	if err != nil {
		return RESULT_ERR, err
	}
	switch mut.Op {
	case CF_PUT:
		err = cfbucket.Put(mut.Key, mut.Value)
	case CF_DEL:
		err = cfbucket.Delete(mut.Key)
	}
	if err != nil {
		return RESULT_ERR, err
	}
	return RESULT_OK, nil
}
