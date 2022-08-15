package kv

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/blacktear23/bolt"
	"github.com/blacktear23/dragonbolt/mvcc"
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

func NewDiskKV(clusterID uint64, nodeID uint64, db *bolt.DB) *DiskKV {
	return &DiskKV{
		clusterID: clusterID,
		nodeID:    nodeID,
		db:        db,
	}
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
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(val), nil
}

func (d *DiskKV) checkColumnFamily(cf string) ([]byte, error) {
	switch cf {
	case CFData, CFLock, CFWrite:
		return []byte(cf), nil
	default:
		return nil, ErrInvalidColumnFamily
	}
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
func (d *DiskKV) Lookup(q interface{}) (interface{}, error) {
	query, ok := q.(*Query)
	if !ok {
		return nil, ErrInvalidQuery
	}
	switch query.Op {
	case GET, GET_VALUE:
		return d.processGet(query)
	case SCAN, SCAN_KEY, SCAN_VALUE:
		return d.processScan(query)
	case CF_GET, CF_GET_VALUE:
		return d.processCfGet(query)
	case CF_SCAN, CF_SCAN_KEY, CF_SCAN_VALUE:
		return d.processCfScan(query)
	case CF_RSCAN, CF_RSCAN_KEY, CF_RSCAN_VALUE:
		return d.processCfReverseScan(query)
	case MVCC_GET, MVCC_GET_VALUE:
		return d.processMvccGet(query)
	case MVCC_SCAN, MVCC_SCAN_KEY, MVCC_SCAN_VALUE:
		return d.processMvccScan(query)
	}
	return nil, ErrUnkonwnQueryOperation
}

func keyCompare(val1 []byte, val2 []byte) int {
	if val2 == nil {
		return -1
	}
	return bytes.Compare(val1, val2)
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
	for idx, ent := range ents {
		result, err := d.processEntry(ent)
		if err != nil {
			return nil, err
		}
		ents[idx].Result = result
	}
	err := d.updateAppliedIndex(ents[len(ents)-1].Index)
	if err != nil {
		return nil, err
	}
	if d.lastApplied >= ents[len(ents)-1].Index {
		panic("lastApplied not moving forward")
	}
	d.lastApplied = ents[len(ents)-1].Index
	return ents, nil
}

func (d *DiskKV) updateAppliedIndex(idx uint64) error {
	return d.db.Update(func(txn *bolt.Tx) error {
		bucket := txn.Bucket(d.bucketName)
		if bucket == nil {
			return ErrBucketNotExists
		}
		appliedIndex := make([]byte, 8)
		binary.LittleEndian.PutUint64(appliedIndex, idx)
		return bucket.Put([]byte(appliedIndexKey), appliedIndex)
	})
}

func isDirectReturnError(err error) bool {
	if mvcc.IsKeyLockedError(err) {
		return false
	}
	if mvcc.IsTxnConflictError(err) {
		return false
	}
	return true
}

func (d *DiskKV) processEntry(ent sm.Entry) (sm.Result, error) {
	var result sm.Result
	err := d.db.Update(func(txn *bolt.Tx) error {
		bucket := txn.Bucket(d.bucketName)
		if bucket == nil {
			return ErrBucketNotExists
		}
		mutations := make([]Mutation, 0, 10)
		if err := json.Unmarshal(ent.Cmd, &mutations); err != nil {
			panic(err)
		}
		ret, perr := d.processMutations(txn, bucket, mutations)
		if perr != nil && isDirectReturnError(perr) {
			return perr
		}
		result = sm.Result{Value: ret}
		return perr
	})
	// If key locked or txn conflict, just return result and no error
	if mvcc.IsKeyLockedError(err) || mvcc.IsTxnConflictError(err) {
		return result, nil
	}
	return result, err
}

func (d *DiskKV) processMutations(txn *bolt.Tx, bucket *bolt.Bucket, muts []Mutation) (uint64, error) {
	var (
		result uint64
		err    error
	)
	for _, mut := range muts {
		switch mut.Op {
		case CF_PUT, CF_DEL:
			result, err = d.processCFMutation(txn, mut)
		case MVCC_SET, MVCC_DEL, MVCC_LOCK, MVCC_UNLOCK, MVCC_UNLOCK_FORCE:
			result, err = d.processMvccMutation(txn, mut)
		default:
			result, err = d.processMutation(bucket, mut)
		}
		if result != RESULT_OK {
			return result, err
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
