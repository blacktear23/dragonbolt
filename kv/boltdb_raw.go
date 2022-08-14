package kv

import (
	"bytes"

	"github.com/blacktear23/bolt"
)

func (d *DiskKV) processGet(query *Query) (*QueryResult, error) {
	ret := &QueryResult{}
	err := d.db.View(func(txn *bolt.Tx) error {
		bucket := txn.Bucket(d.bucketName)
		if bucket == nil {
			return ErrBucketNotExists
		}
		for _, key := range query.Keys {
			val := bucket.Get(key)
			ret.KVS = append(ret.KVS, buildKVP(key, val, query.Op))
		}
		return nil
	})
	if err == nil && d.closed {
		panic("lookup returned valid result when DiskKV is already closed")
	}
	return ret, err
}

func (d *DiskKV) processScan(query *Query) (*QueryResult, error) {
	// Scan for [Start, End)
	ret := &QueryResult{}

	err := d.db.View(func(txn *bolt.Tx) error {
		bucket := txn.Bucket(d.bucketName)
		if bucket == nil {
			return ErrBucketNotExists
		}
		c := bucket.Cursor()
		// Check for first result
		k, v := c.Seek(query.Start)
		// Check for rest results
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

func (d *DiskKV) processMutation(bucket *bolt.Bucket, mut Mutation) (uint64, error) {
	var err error
	switch mut.Op {
	case PUT:
		err = bucket.Put(mut.Key, mut.Value)
	case DEL:
		err = bucket.Delete(mut.Key)
	case CAS:
		cval := bucket.Get(mut.Key)
		// Compare for nil
		if cval == nil && mut.Value != nil {
			return RESULT_FAIL, nil
		} else if cval != nil && mut.Value == nil {
			return RESULT_FAIL, nil
		}
		if !bytes.Equal(cval, mut.Value) {
			return RESULT_FAIL, nil
		}
		err = bucket.Put(mut.Key, mut.NewValue)
	}
	if err != nil {
		return RESULT_ERR, err
	}
	return RESULT_OK, nil
}
