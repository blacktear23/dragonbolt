package tso

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/blacktear23/bolt"
	"github.com/blacktear23/dragonbolt/kv"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

var (
	_               sm.IOnDiskStateMachine = (sm.IOnDiskStateMachine)(&TSOKV{})
	appliedIndexKey []byte                 = []byte("m_applied_index")
)

type TSOKVBuilder struct {
	DB *bolt.DB
}

func (b *TSOKVBuilder) Build(clusterID uint64, nodeID uint64) sm.IOnDiskStateMachine {
	return &TSOKV{
		clusterID: clusterID,
		nodeID:    nodeID,
		db:        b.DB,
	}
}

type TSOKV struct {
	clusterID   uint64
	nodeID      uint64
	lastApplied uint64
	db          *bolt.DB
	bucketName  []byte
	closed      bool
	aborted     bool
}

func (m *TSOKV) getKey(key []byte) ([]byte, error) {
	var ret []byte
	terr := m.db.View(func(txn *bolt.Tx) error {
		bucket := txn.Bucket(m.bucketName)
		if bucket == nil {
			return kv.ErrBucketNotExists
		}
		ret = bucket.Get(key)
		return nil
	})
	return ret, terr
}

func (m *TSOKV) queryAppliedIndex() (uint64, error) {
	val, err := m.getKey(appliedIndexKey)
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(val), nil
}

func (m *TSOKV) Open(stopc <-chan struct{}) (uint64, error) {
	m.bucketName = []byte(fmt.Sprintf("tso_%d", m.clusterID))
	err := m.db.Update(func(txn *bolt.Tx) error {
		_, err := txn.CreateBucketIfNotExists(m.bucketName)
		return err
	})
	if err != nil {
		return 0, err
	}
	appliedIndex, err := m.queryAppliedIndex()
	if err != nil {
		panic(err)
	}
	m.lastApplied = appliedIndex
	return appliedIndex, nil
}

func (m *TSOKV) Lookup(key interface{}) (interface{}, error) {
	v, err := m.getKey(key.([]byte))
	if err == nil && m.closed {
		panic("lookup returned valid result when TSOKV is already closed")
	}
	return v, err
}

func (m *TSOKV) checkStatus() {
	if m.aborted {
		panic("update() called after abort set to true")
	}
	if m.closed {
		panic("update called after Close()")
	}
}

func (m *TSOKV) Update(ents []sm.Entry) ([]sm.Entry, error) {
	m.checkStatus()
	err := m.db.Update(func(txn *bolt.Tx) error {
		bucket := txn.Bucket(m.bucketName)
		if bucket == nil {
			return kv.ErrBucketNotExists
		}
		for idx, e := range ents {
			req := &tsoRequest{}
			if err := json.Unmarshal(e.Cmd, req); err != nil {
				log.Println(err)
				return err
			}
			result, perr := m.processRequest(bucket, req)
			if perr != nil {
				return perr
			}
			ents[idx].Result = sm.Result{Value: result}
		}
		// save the applied index to the DB.
		appliedIndex := make([]byte, 8)
		binary.LittleEndian.PutUint64(appliedIndex, ents[len(ents)-1].Index)
		return bucket.Put(appliedIndexKey, appliedIndex)
	})
	if err != nil {
		return nil, err
	}
	if m.lastApplied >= ents[len(ents)-1].Index {
		panic("lastApplied not moving forward")
	}
	m.lastApplied = ents[len(ents)-1].Index
	return ents, nil
}

func (m *TSOKV) processRequest(bucket *bolt.Bucket, req *tsoRequest) (uint64, error) {
	var tso uint64 = 0
	tval := bucket.Get(req.Key)
	if tval != nil {
		tso = binary.LittleEndian.Uint64(tval)
	}
	tso += 1
	newTSO := make([]byte, 8)
	binary.LittleEndian.PutUint64(newTSO, tso)
	err := bucket.Put(req.Key, newTSO)
	if err != nil {
		return 0, err
	}
	return tso, nil
}

func (m *TSOKV) Sync() error {
	return m.db.Sync()
}

func (m *TSOKV) PrepareSnapshot() (interface{}, error) {
	m.checkStatus()
	return nil, nil
}

func (m *TSOKV) SaveSnapshot(ctx interface{}, w io.Writer, done <-chan struct{}) error {
	m.checkStatus()
	return m.db.View(func(txn *bolt.Tx) error {
		snapshot := Snapshot{}
		bucket := txn.Bucket(m.bucketName)
		if bucket == nil {
			return kv.ErrBucketNotExists
		}
		bucket.ForEach(func(key []byte, val []byte) error {
			snapshot.KVS = append(snapshot.KVS, KVPair{Key: key, Value: val})
			return nil
		})
		enc := json.NewEncoder(w)
		err := enc.Encode(snapshot)
		if err != nil {
			return err
		}
		return nil
	})
}

func (m *TSOKV) RecoverFromSnapshot(r io.Reader, done <-chan struct{}) error {
	if m.closed {
		panic("recover from snapshot called after Close()")
	}
	snapshot := Snapshot{}
	dec := json.NewDecoder(r)
	err := dec.Decode(&snapshot)
	if err != nil {
		return err
	}
	return m.db.Update(func(txn *bolt.Tx) error {
		bucket, err := txn.CreateBucketIfNotExists(m.bucketName)
		if err != nil {
			return err
		}
		for _, kvp := range snapshot.KVS {
			err = bucket.Put(kvp.Key, kvp.Value)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (m *TSOKV) Close() error {
	return m.db.Close()
}

type KVPair struct {
	Key   []byte `json:"key"`
	Value []byte `json:"val"`
}

type Snapshot struct {
	KVS []KVPair `json:"kvs"`
}
