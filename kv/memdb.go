package kv

import (
	"bytes"
	"encoding/json"
	"io"
	"sync"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

type MemKV struct {
	data        sync.Map
	clusterID   uint64
	nodeID      uint64
	lastApplied uint64
	closed      bool
	aborted     bool
}

func (m *MemKV) GetKey(key []byte) ([]byte, error) {
	ret, have := m.data.Load(key)
	if !have {
		return nil, nil
	}
	return ret.([]byte), nil
}

func (m *MemKV) PutKeyValue(key []byte, value []byte) error {
	m.data.Store(string(key), value)
	return nil
}

func (m *MemKV) Open(stopc <-chan struct{}) (uint64, error) {
	return 0, nil
}

func (m *MemKV) Lookup(key interface{}) (interface{}, error) {
	skey := string(key.([]byte))
	ret, have := m.data.Load(skey)
	if !have {
		return nil, nil
	}
	return ret, nil
}

func (m *MemKV) Update(ents []sm.Entry) ([]sm.Entry, error) {
	if m.aborted {
		panic("update() called after abort set to true")
	}
	if m.closed {
		panic("update called after Close()")
	}
	for idx, e := range ents {
		muts := []Mutation{}
		if err := json.Unmarshal(e.Cmd, muts); err != nil {
			panic(err)
		}
		result, perr := m.processMutations(muts)
		if perr != nil {
			return nil, perr
		}
		ents[idx].Result = sm.Result{Value: result}
	}
	if m.lastApplied >= ents[len(ents)-1].Index {
		panic("lastApplied not moving forward")
	}
	m.lastApplied = ents[len(ents)-1].Index
	return ents, nil
}

func (m *MemKV) processMutations(muts []Mutation) (uint64, error) {
	for _, mut := range muts {
		result, err := m.processMutation(mut)
		if result != RESULT_OK {
			return result, err
		}
	}
	return RESULT_OK, nil
}

func (m *MemKV) processMutation(mut Mutation) (uint64, error) {
	switch mut.Op {
	case PUT:
		key := string(mut.Key)
		m.data.Store(key, mut.Value)
	case DEL:
		key := string(mut.Key)
		m.data.Delete(key)
	case CAS:
		key := string(mut.Key)
		cval, _ := m.data.Load(key)
		// Compare for nil
		if cval == nil && mut.Value != nil {
			return RESULT_FAIL, nil
		} else if cval != nil && mut.Value == nil {
			return RESULT_FAIL, nil
		}
		bval := cval.([]byte)
		if !bytes.Equal(bval, mut.Value) {
			return RESULT_FAIL, nil
		}
		m.data.Store(key, mut.NewValue)
	}
	return RESULT_OK, nil
}

func (m *MemKV) Sync() error {
	return nil
}

// PrepareSnapshot prepares snapshotting. PrepareSnapshot is responsible to
// capture a state identifier that identifies a point in time state of the
// underlying data. In this example, we use Pebble's snapshot feature to
// achieve that.
func (m *MemKV) PrepareSnapshot() (interface{}, error) {
	if m.closed {
		panic("prepare snapshot called after Close()")
	}
	if m.aborted {
		panic("prepare snapshot called after abort")
	}
	return nil, nil
}

// SaveSnapshot saves the state machine state identified by the state
// identifier provided by the input ctx parameter. Note that SaveSnapshot
// is not suppose to save the latest state.
func (m *MemKV) SaveSnapshot(ctx interface{}, w io.Writer, done <-chan struct{}) error {
	if m.closed {
		panic("prepare snapshot called after Close()")
	}
	if m.aborted {
		panic("prepare snapshot called after abort")
	}
	snap := make(map[string][]byte)
	m.data.Range(func(k, v any) bool {
		snap[k.(string)] = v.([]byte)
		return true
	})
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

// RecoverFromSnapshot recovers the state machine state from snapshot. The
// snapshot is recovered into a new DB first and then atomically swapped with
// the existing DB to complete the recovery.
func (m *MemKV) RecoverFromSnapshot(r io.Reader, done <-chan struct{}) error {
	if m.closed {
		panic("recover from snapshot called after Close()")
	}
	snap := make(map[string][]byte)
	dec := json.NewDecoder(r)
	err := dec.Decode(&snap)
	if err != nil {
		return err
	}
	for k, v := range snap {
		m.data.Store(k, v)
	}
	return nil
}

func (m *MemKV) Close() error {
	return nil
}
