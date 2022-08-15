package store

import (
	"sync"
)

type StoreManager struct {
	Dir    string
	Stores sync.Map
	Join   bool
}

func NewStoreManager(dir string, join bool) *StoreManager {
	return &StoreManager{
		Dir:    dir,
		Stores: sync.Map{},
		Join:   join,
	}
}

func (m *StoreManager) CreateStore(storeID uint64) (*BoltDBStore, error) {
	stor, err := NewBoltDBStore(m.Dir, storeID, m.Join)
	if err != nil {
		return nil, err
	}
	m.Stores.Store(storeID, stor)
	return stor, err
}

func (m *StoreManager) RemoveStore(storeID uint64) error {
	val, have := m.Stores.LoadAndDelete(storeID)
	if have {
		if stor, ok := val.(*BoltDBStore); ok {
			err := stor.Close()
			if err != nil {
				return err
			}
			return stor.Delete()
		}
	}
	return nil
}

func (m *StoreManager) Close() error {
	var err error = nil
	m.Stores.Range(func(key, val any) bool {
		if stor, ok := val.(*BoltDBStore); ok {
			cerr := stor.Close()
			if err == nil {
				err = cerr
			}
		}
		return true
	})
	return err
}
