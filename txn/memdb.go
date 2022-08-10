package txn

import (
	"github.com/blacktear23/dragonbolt/kv"
	omap "github.com/wk8/go-ordered-map/v2"
)

type Iter interface {
	Seek([]byte) (key []byte, value []byte)
	Next() (key []byte, value []byte)
}

type MemDB struct {
	muts []kv.Mutation
	view *omap.OrderedMap[string, []byte]
	dels map[string]bool
}

func NewMemDB() *MemDB {
	return &MemDB{
		muts: []kv.Mutation{},
		view: omap.New[string, []byte](),
		dels: make(map[string]bool),
	}
}

func (db *MemDB) Get(key []byte) ([]byte, bool) {
	skey := string(key)
	if _, have := db.dels[skey]; have {
		// set nil and have means memdb has it
		return nil, true
	}
	return db.view.Get(string(key))
}

func (db *MemDB) Set(key []byte, value []byte) error {
	skey := string(key)
	db.addmut(kv.PUT, key, value)
	db.view.Store(skey, value)
	if _, have := db.dels[skey]; have {
		delete(db.dels, skey)
	}
	return nil
}

func (db *MemDB) Delete(key []byte) error {
	skey := string(key)
	db.addmut(kv.DEL, key, nil)
	db.view.Delete(skey)
	db.dels[skey] = true
	return nil
}

func clone(val []byte) []byte {
	ret := make([]byte, len(val))
	copy(ret, val)
	return ret
}

func (db *MemDB) addmut(op int, key []byte, value []byte) {
	ckey := clone(key)
	cvalue := clone(value)
	mut := kv.Mutation{
		Op:    op,
		Key:   ckey,
		Value: cvalue,
	}
	db.muts = append(db.muts, mut)
}

func (db *MemDB) GetMutations() []kv.Mutation {
	return db.muts
}

func (db *MemDB) Iter() Iter {
	return &memdbIter{
		db: db,
	}
}

type memdbIter struct {
	db      *MemDB
	current *omap.Pair[string, []byte]
}

func (i *memdbIter) Seek(key []byte) ([]byte, []byte) {
	pair := i.db.view.GetPair(string(key))
	i.current = pair
	if pair == nil {
		return nil, nil
	}
	return []byte(pair.Key), pair.Value
}

func (i *memdbIter) Next() ([]byte, []byte) {
	if i.current == nil {
		return nil, nil
	}
	next := i.current.Next()
	i.current = next
	if next == nil {
		return nil, nil
	}
	return []byte(next.Key), next.Value
}
