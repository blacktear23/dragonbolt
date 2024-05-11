package query

import (
	"github.com/blacktear23/dragonbolt/kv"
	"github.com/blacktear23/dragonbolt/txn"
	"github.com/c4pt0r/kvql"
)

var (
	_ kvql.Storage = &TxnStore{}
)

type TxnStore struct {
	txn txn.Txn
}

func NewTxnStore(txn txn.Txn) *TxnStore {
	return &TxnStore{
		txn: txn,
	}
}

func (s *TxnStore) Get(key []byte) ([]byte, error) {
	return s.txn.Get(key)
}

func (s *TxnStore) Put(key []byte, value []byte) error {
	return s.txn.Set(key, value)
}

func (s *TxnStore) BatchPut(kvs []kvql.KVPair) error {
	kvps := make([]kv.KVPair, len(kvs))
	for i, kva := range kvs {
		kvp := kv.KVPair{Key: kva.Key, Value: kva.Value}
		kvps[i] = kvp
	}
	return s.txn.BatchSet(kvps)
}

func (s *TxnStore) Delete(key []byte) error {
	return s.txn.Delete(key)
}

func (s *TxnStore) BatchDelete(keys [][]byte) error {
	for _, key := range keys {
		err := s.Delete(key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *TxnStore) Cursor() (kvql.Cursor, error) {
	return s.txn.Cursor()
}
