package txn

import (
	"bytes"
	"errors"

	"github.com/blacktear23/dragonbolt/kv"
	"github.com/igrmk/treemap/v2"
)

var (
	ErrAlreadyHaveSavepoint    = errors.New("Already have savepoint")
	ErrSavepointNotExists      = errors.New("Savepoint not exists")
	ErrCannotRollbackSavepoint = errors.New("Cannot rollback savepoint")
)

type Iter interface {
	Seek([]byte) (key []byte, value []byte)
	Next() (key []byte, value []byte)
}

type opLogs struct {
	logsIdx []int
}

func newOpLogs() *opLogs {
	return &opLogs{
		logsIdx: make([]int, 0, 10),
	}
}

func (l *opLogs) Push(idx int) {
	l.logsIdx = append(l.logsIdx, idx)
}

func (l *opLogs) GetLast() int {
	idx := len(l.logsIdx) - 1
	return l.logsIdx[idx]
}

func (l *opLogs) popTo(idx int) int {
	newLog := make([]int, 0, len(l.logsIdx))
	for _, i := range l.logsIdx {
		if i <= idx {
			newLog = append(newLog, i)
		}
	}
	l.logsIdx = newLog
	return len(newLog)
}

type MemDB struct {
	PutOp      int
	DelOp      int
	Version    uint64
	muts       []kv.Mutation
	view       *treemap.TreeMap[[]byte, *opLogs]
	savepoints map[string]int
}

func NewMemDB(putOp int, delOp int, ver uint64) *MemDB {
	return &MemDB{
		muts: []kv.Mutation{},
		view: treemap.NewWithKeyCompare[[]byte, *opLogs](func(a []byte, b []byte) bool {
			return bytes.Compare(a, b) < 0
		}),
		PutOp:      putOp,
		DelOp:      delOp,
		Version:    ver,
		savepoints: map[string]int{},
	}
}

func (db *MemDB) Get(key []byte) ([]byte, bool) {
	log, have := db.view.Get(key)
	if !have {
		return nil, false
	}
	op, val := db.getLogVal(key, log.GetLast())
	if op == db.DelOp {
		return nil, false
	}
	return val, true
}

func (db *MemDB) CreateSavepoint(name string) error {
	mutIdx := len(db.muts) - 1
	_, have := db.savepoints[name]
	if have {
		return ErrAlreadyHaveSavepoint
	}
	db.savepoints[name] = mutIdx
	return nil
}

func (db *MemDB) DeleteSavepoint(name string) error {
	_, have := db.savepoints[name]
	if !have {
		return ErrSavepointNotExists
	}
	delete(db.savepoints, name)
	return nil
}

func (db *MemDB) RollbackToSavepoint(name string) error {
	lastIdx, have := db.savepoints[name]
	if !have {
		return ErrSavepointNotExists
	}
	if lastIdx >= len(db.muts) {
		return ErrCannotRollbackSavepoint
	}
	return db.rollbackTo(lastIdx)
}

func (db *MemDB) rollbackTo(idx int) error {
	// first rollback views
	needDel := [][]byte{}
	for it := db.view.Iterator(); it.Valid(); it.Next() {
		key, logs := it.Key(), it.Value()
		restNum := logs.popTo(idx)
		if restNum == 0 {
			needDel = append(needDel, key)
		}
	}
	for _, key := range needDel {
		db.view.Del(key)
	}
	// pop muts
	db.muts = db.muts[0 : idx+1]
	return nil
}

func (db *MemDB) getLogVal(key []byte, idx int) (int, []byte) {
	if idx >= len(db.muts) {
		return db.DelOp, nil
	}
	mut := db.muts[idx]
	if !bytes.Equal(mut.Key, key) {
		return db.DelOp, nil
	}
	return mut.Op, mut.Value
}

func (db *MemDB) updateView(key []byte, idx int) {
	logs, have := db.view.Get(key)
	if !have {
		logs = newOpLogs()
		db.view.Set(key, logs)
	}
	logs.Push(idx)
}

func (db *MemDB) Set(key []byte, value []byte) error {
	ckey := clone(key)
	cval := clone(value)
	idx := db.addmut(db.PutOp, ckey, cval)
	db.updateView(ckey, idx)
	return nil
}

func (db *MemDB) Delete(key []byte) error {
	ckey := clone(key)
	idx := db.addmut(db.DelOp, ckey, nil)
	db.updateView(ckey, idx)
	return nil
}

func clone(val []byte) []byte {
	ret := make([]byte, len(val))
	copy(ret, val)
	return ret
}

func (db *MemDB) addmut(op int, key []byte, value []byte) int {
	mut := kv.Mutation{
		Op:    op,
		Key:   key,
		Value: value,
	}
	if db.Version != 0 {
		mut.Version = db.Version
		mut.CommitVersion = db.Version
	}
	db.muts = append(db.muts, mut)
	return len(db.muts) - 1
}

func (db *MemDB) GetMutations(commitVer uint64) []kv.Mutation {
	if commitVer == 0 {
		return db.muts
	}
	for i, _ := range db.muts {
		db.muts[i].CommitVersion = commitVer
	}
	return db.muts
}

func (db *MemDB) Iter() Iter {
	return &memdbIter{
		db:   db,
		iter: db.view.Iterator(),
	}
}

func (db *MemDB) IsDelete(key []byte) bool {
	log, have := db.view.Get(key)
	if !have {
		return false
	}
	op, _ := db.getLogVal(key, log.GetLast())
	return op == db.DelOp
}

type memdbIter struct {
	db   *MemDB
	iter treemap.ForwardIterator[[]byte, *opLogs]
}

func (i *memdbIter) Seek(key []byte) ([]byte, []byte) {
	i.iter = i.db.view.LowerBound(key)
	for {
		if !i.iter.Valid() {
			break
		}
		key := i.iter.Key()
		logs := i.iter.Value()
		op, val := i.db.getLogVal(key, logs.GetLast())
		if op == i.db.PutOp {
			return key, val
		}
		i.iter.Next()
	}
	return nil, nil
}

func (i *memdbIter) Next() ([]byte, []byte) {
	i.iter.Next()
	for {
		if !i.iter.Valid() {
			break
		}
		key := i.iter.Key()
		logs := i.iter.Value()
		op, val := i.db.getLogVal(key, logs.GetLast())
		if op == i.db.PutOp {
			return key, val
		}
		i.iter.Next()
	}
	return nil, nil
}
