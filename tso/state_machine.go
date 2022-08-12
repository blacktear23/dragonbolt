package tso

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/blacktear23/bolt"
	"github.com/blacktear23/dragonbolt/kv"
	"github.com/blacktear23/dragonbolt/store"
	"github.com/lni/dragonboat/v4"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

var (
	_               sm.IOnDiskStateMachine = (sm.IOnDiskStateMachine)(&TSOKV{})
	appliedIndexKey []byte                 = []byte("m_applied_index")

	ErrInvalidQuery          = errors.New("Invalid query")
	ErrUnknownQueryOperation = errors.New("Unknown query operation")

	dbPrefix []byte = []byte("db_")
)

type TSOKVBuilder struct {
	DB           *bolt.DB
	NodeHost     *dragonboat.NodeHost
	StoreManager *store.StoreManager
	InitMembers  map[uint64]string
	ReplicaID    uint64
}

func (b *TSOKVBuilder) Build(clusterID uint64, nodeID uint64) sm.IOnDiskStateMachine {
	return &TSOKV{
		clusterID: clusterID,
		nodeID:    nodeID,
		db:        b.DB,
		nh:        b.NodeHost,
		sm:        b.StoreManager,
		members:   b.InitMembers,
		replicaID: b.ReplicaID,
	}
}

type TSOKV struct {
	clusterID   uint64
	nodeID      uint64
	lastApplied uint64
	db          *bolt.DB
	nh          *dragonboat.NodeHost
	sm          *store.StoreManager
	bucketName  []byte
	members     map[uint64]string
	replicaID   uint64
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

func (m *TSOKV) Lookup(req interface{}) (interface{}, error) {
	query, ok := req.(*tsoQuery)
	if !ok {
		return nil, ErrInvalidQuery
	}
	var (
		ret any
		err error
	)
	switch query.Op {
	case OP_QUERY_DBS:
		ret, err = m.queryDbInfoList()
	default:
		return nil, ErrUnknownQueryOperation
	}
	if err == nil && m.closed {
		panic("lookup returned valid result when TSOKV is already closed")
	}
	return ret, err
}

func (m *TSOKV) queryDbInfoList() (*tsoQueryResult, error) {
	ret := []*DBInfo{}
	err := m.db.View(func(txn *bolt.Tx) error {
		bucket := txn.Bucket(m.bucketName)
		if bucket == nil {
			return kv.ErrBucketNotExists
		}
		c := bucket.Cursor()
		k, v := c.Seek(dbPrefix)
		if k == nil {
			// Not found
			return nil
		}
		if bytes.HasPrefix(k, dbPrefix) {
			dbi, err := DBInfoFromBytes(v)
			if err == nil {
				ret = append(ret, dbi)
			}
		} else {
			// End of list
			return nil
		}
		for {
			k, v = c.Next()
			if k == nil {
				// Not found
				return nil
			}
			if bytes.HasPrefix(k, dbPrefix) {
				dbi, err := DBInfoFromBytes(v)
				if err == nil {
					ret = append(ret, dbi)
				}
			} else {
				// End of list
				return nil
			}
		}
	})
	return &tsoQueryResult{
		DBInfoList: ret,
	}, err
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
	switch req.Op {
	case OP_TSO:
		return m.handleTso(bucket, req)
	case OP_CREATE_DB:
		return m.handleCreateDB(bucket, req)
	case OP_DELETE_DB:
		return m.handleDeleteDB(bucket, req)
	case OP_UP_DB:
		return m.handleUpDB(bucket, req)
	case OP_DOWN_DB:
		return m.handleDownDB(bucket, req)
	default:
		return 0, nil
	}
}

func DBInfoFromBytes(data []byte) (*DBInfo, error) {
	ret := &DBInfo{}
	err := json.Unmarshal(data, ret)
	return ret, err
}

func (m *TSOKV) handleUpDB(bucket *bolt.Bucket, req *tsoRequest) (uint64, error) {
	sid := req.ShardID
	stor, err := m.sm.CreateStore(sid)
	if err != nil {
		log.Println("Cannot create store", err)
		return 0, nil
	}
	err = stor.StartReplica(m.nh, m.members, m.replicaID, false)
	if err != nil {
		log.Println("Cannot start replica store", err)
		return 0, nil
	}
	return 1, nil
}

func (m *TSOKV) handleDownDB(bucket *bolt.Bucket, req *tsoRequest) (uint64, error) {
	sid := req.ShardID
	err := m.nh.StopReplica(sid, m.replicaID)
	if err != nil {
		log.Println("Cannot stop replica", err)
		return 0, nil
	}
	err = m.sm.RemoveStore(sid)
	if err != nil {
		log.Println("Cannot remove store", err)
		return 0, nil
	}
	return 1, nil
}

func (m *TSOKV) handleCreateDB(bucket *bolt.Bucket, req *tsoRequest) (uint64, error) {
	key := fmt.Sprintf("db_%s", req.Name)
	info := &DBInfo{
		Name:    req.Name,
		ShardID: req.ShardID,
	}
	value, err := json.Marshal(info)
	if err != nil {
		return 0, err
	}
	err = bucket.Put([]byte(key), value)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

func (m *TSOKV) handleDeleteDB(bucket *bolt.Bucket, req *tsoRequest) (uint64, error) {
	key := fmt.Sprintf("db_%s", req.Name)
	err := bucket.Delete([]byte(key))
	if err != nil {
		return 0, err
	}
	return 1, nil
}

func (m *TSOKV) handleTso(bucket *bolt.Bucket, req *tsoRequest) (uint64, error) {
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
