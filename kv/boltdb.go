package kv

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/blacktear23/bolt"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

// DiskKV is a state machine that implements the IOnDiskStateMachine interface.
// DiskKV stores key-value pairs in the underlying PebbleDB key-value store. As
// it is used as an example, it is implemented using the most basic features
// common in most key-value stores. This is NOT a benchmark program.
type DiskKV struct {
	clusterID   uint64
	nodeID      uint64
	lastApplied uint64
	db          *bolt.DB
	bucketName  []byte
	closed      bool
	aborted     bool
}

func (d *DiskKV) GetKey(key []byte) ([]byte, error) {
	var ret []byte
	terr := d.db.View(func(txn *bolt.Tx) error {
		bucket := txn.Bucket(d.bucketName)
		if bucket == nil {
			return ErrBucketNotExists
		}
		ret = bucket.Get(key)
		return nil
	})
	return ret, terr
}

func (d *DiskKV) PutKeyValue(key []byte, value []byte) error {
	return d.db.Update(func(txn *bolt.Tx) error {
		bucket := txn.Bucket(d.bucketName)
		if bucket == nil {
			return ErrBucketNotExists
		}
		return bucket.Put(key, value)
	})
}

func (d *DiskKV) queryAppliedIndex() (uint64, error) {
	val, err := d.GetKey([]byte(appliedIndexKey))
	log.Printf("[DEBUG] %v, %v", val, err)
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(val), nil
}

// Open opens the state machine and return the index of the last Raft Log entry
// already updated into the state machine.
func (d *DiskKV) Open(stopc <-chan struct{}) (uint64, error) {
	d.bucketName = []byte(fmt.Sprintf("bucket_cid_%d", d.clusterID))
	err := d.db.Update(func(txn *bolt.Tx) error {
		_, err := txn.CreateBucketIfNotExists(d.bucketName)
		return err
	})
	if err != nil {
		return 0, err
	}
	appliedIndex, err := d.queryAppliedIndex()
	if err != nil {
		panic(err)
	}
	d.lastApplied = appliedIndex
	return appliedIndex, nil
}

// Lookup queries the state machine.
func (d *DiskKV) Lookup(key interface{}) (interface{}, error) {
	v, err := d.GetKey(key.([]byte))
	if err == nil && d.closed {
		panic("lookup returned valid result when DiskKV is already closed")
	}
	return v, err
}

// Update updates the state machine. In this example, all updates are put into
// a PebbleDB write batch and then atomically written to the DB together with
// the index of the last Raft Log entry. For simplicity, we always Sync the
// writes (db.wo.Sync=True). To get higher throughput, you can implement the
// Sync() method below and choose not to synchronize for every Update(). Sync()
// will periodically called by Dragonboat to synchronize the state.
func (d *DiskKV) Update(ents []sm.Entry) ([]sm.Entry, error) {
	// log.Println("[I] Start Update")
	if d.aborted {
		panic("update() called after abort set to true")
	}
	if d.closed {
		panic("update called after Close()")
	}
	err := d.db.Update(func(txn *bolt.Tx) error {
		bucket := txn.Bucket(d.bucketName)
		if bucket == nil {
			return ErrBucketNotExists
		}
		for idx, e := range ents {
			mutation := &Mutation{}
			if err := json.Unmarshal(e.Cmd, mutation); err != nil {
				panic(err)
			}
			result, perr := d.processMutation(bucket, mutation)
			if perr != nil {
				return perr
			}
			ents[idx].Result = sm.Result{Value: result}
		}
		// save the applied index to the DB.
		appliedIndex := make([]byte, 8)
		binary.LittleEndian.PutUint64(appliedIndex, ents[len(ents)-1].Index)
		return bucket.Put([]byte(appliedIndexKey), appliedIndex)
	})
	if err != nil {
		return nil, err
	}
	if d.lastApplied >= ents[len(ents)-1].Index {
		panic("lastApplied not moving forward")
	}
	d.lastApplied = ents[len(ents)-1].Index
	// log.Println("[I] End Update")
	return ents, nil
}

func (d *DiskKV) processMutation(bucket *bolt.Bucket, mut *Mutation) (uint64, error) {
	kvs := len(mut.Keys)
	switch mut.Op {
	case PUT:
		for i := 0; i < kvs; i++ {
			err := bucket.Put(mut.Keys[i], mut.Values[i])
			if err != nil {
				return RESULT_ERR, err
			}
		}
	case DEL:
		for i := 0; i < kvs; i++ {
			err := bucket.Delete(mut.Keys[i])
			if err != nil {
				return RESULT_ERR, err
			}
		}
	case CAS:
		// Check for old value equals
		for i := 0; i < kvs; i++ {
			cval := bucket.Get(mut.Keys[i])
			// Compare for nil
			if cval == nil && mut.Values[i] != nil {
				return RESULT_FAIL, nil
			} else if cval != nil && mut.Values[i] == nil {
				return RESULT_FAIL, nil
			}
			if !bytes.Equal(cval, mut.Values[i]) {
				return RESULT_FAIL, nil
			}
		}
		for i := 0; i < kvs; i++ {
			err := bucket.Put(mut.Keys[i], mut.NewValues[i])
			if err != nil {
				return RESULT_ERR, err
			}
		}
	}
	return RESULT_OK, nil
}

// Sync synchronizes all in-core state of the state machine. Since the Update
// method in this example already does that every time when it is invoked, the
// Sync method here is a NoOP.
func (d *DiskKV) Sync() error {
	return d.db.Sync()
}

// PrepareSnapshot prepares snapshotting. PrepareSnapshot is responsible to
// capture a state identifier that identifies a point in time state of the
// underlying data. In this example, we use Pebble's snapshot feature to
// achieve that.
func (d *DiskKV) PrepareSnapshot() (interface{}, error) {
	if d.closed {
		panic("prepare snapshot called after Close()")
	}
	if d.aborted {
		panic("prepare snapshot called after abort")
	}
	return nil, nil
}

// SaveSnapshot saves the state machine state identified by the state
// identifier provided by the input ctx parameter. Note that SaveSnapshot
// is not suppose to save the latest state.
func (d *DiskKV) SaveSnapshot(ctx interface{},
	w io.Writer, done <-chan struct{}) error {
	if d.closed {
		panic("prepare snapshot called after Close()")
	}
	if d.aborted {
		panic("prepare snapshot called after abort")
	}
	return d.db.View(func(txn *bolt.Tx) error {
		_, err := txn.WriteTo(w)
		if err != nil {
			return err
		}
		return nil
	})
}

// RecoverFromSnapshot recovers the state machine state from snapshot. The
// snapshot is recovered into a new DB first and then atomically swapped with
// the existing DB to complete the recovery.
func (d *DiskKV) RecoverFromSnapshot(r io.Reader,
	done <-chan struct{}) error {
	if d.closed {
		panic("recover from snapshot called after Close()")
	}
	fname, err := d.writeToTempFile(r)
	if err != nil {
		return err
	}
	sdb, err := bolt.Open(fname, 0600, nil)
	if err != nil {
		return err
	}
	defer sdb.Close()
	stxn, err := sdb.Begin(false)
	if err != nil {
		return err
	}
	defer stxn.Commit()
	sbucket := stxn.Bucket(d.bucketName)
	// Nothing to do
	if sbucket == nil {
		return nil
	}
	// Overflow all key values.
	return d.db.Update(func(txn *bolt.Tx) error {
		bucket, err := txn.CreateBucketIfNotExists(d.bucketName)
		if err != nil {
			return err
		}
		return sbucket.ForEach(func(key []byte, val []byte) error {
			return bucket.Put(key, val)
		})
	})
}

func (d *DiskKV) writeToTempFile(src io.Reader) (string, error) {
	tmpFile := fmt.Sprintf("/tmp/snap-%d-%d.db", d.clusterID, d.nodeID)
	fp, err := os.OpenFile(tmpFile, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return tmpFile, err
	}
	defer fp.Close()
	_, err = io.Copy(fp, src)
	if err != nil {
		return tmpFile, err
	}
	return tmpFile, nil
}

// Close closes the state machine.
func (d *DiskKV) Close() error {
	return d.db.Close()
}
