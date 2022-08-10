package txn

import (
	"bytes"

	"github.com/blacktear23/dragonbolt/kv"
	"github.com/igrmk/treemap/v2"
)

type Iter interface {
	Seek([]byte) (key []byte, value []byte)
	Next() (key []byte, value []byte)
}

type MemDB struct {
	muts []kv.Mutation
	view *treemap.TreeMap[[]byte, []byte]
	dels map[string]bool
}

func NewMemDB() *MemDB {
	return &MemDB{
		muts: []kv.Mutation{},
		view: treemap.NewWithKeyCompare[[]byte, []byte](func(a []byte, b []byte) bool {
			return bytes.Compare(a, b) < 0
		}),
		dels: make(map[string]bool),
	}
}

func (db *MemDB) Get(key []byte) ([]byte, bool) {
	skey := string(key)
	if _, have := db.dels[skey]; have {
		// set nil and have means memdb has it
		return nil, true
	}
	return db.view.Get(key)
}

func (db *MemDB) Set(key []byte, value []byte) error {
	skey := string(key)
	ckey := clone(key)
	cval := clone(value)
	db.addmut(kv.PUT, ckey, cval)
	db.view.Set(ckey, cval)
	if _, have := db.dels[skey]; have {
		delete(db.dels, skey)
	}
	return nil
}

func (db *MemDB) Delete(key []byte) error {
	skey := string(key)
	ckey := clone(key)
	db.addmut(kv.DEL, ckey, nil)
	db.view.Del(ckey)
	db.dels[skey] = true
	return nil
}

func clone(val []byte) []byte {
	ret := make([]byte, len(val))
	copy(ret, val)
	return ret
}

func (db *MemDB) addmut(op int, key []byte, value []byte) {
	mut := kv.Mutation{
		Op:    op,
		Key:   key,
		Value: value,
	}
	db.muts = append(db.muts, mut)
}

func (db *MemDB) GetMutations() []kv.Mutation {
	return db.muts
}

func (db *MemDB) Iter() Iter {
	return &memdbIter{
		db:   db,
		iter: db.view.Iterator(),
	}
}

func (db *MemDB) IsDelete(key []byte) bool {
	_, have := db.dels[string(key)]
	return have
}

type memdbIter struct {
	db   *MemDB
	iter treemap.ForwardIterator[[]byte, []byte]
}

func (i *memdbIter) Seek(key []byte) ([]byte, []byte) {
	for {
		if !i.iter.Valid() {
			return nil, nil
		}
		if bytes.Compare(i.iter.Key(), key) >= 0 {
			break
		} else {
			i.iter.Next()
		}
	}
	return i.iter.Key(), i.iter.Value()
}

func (i *memdbIter) Next() ([]byte, []byte) {
	i.iter.Next()
	if i.iter.Valid() {
		return i.iter.Key(), i.iter.Value()
	}
	return nil, nil
}
