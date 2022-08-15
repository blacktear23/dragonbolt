package server

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/blacktear23/dragonbolt/kv"
	"github.com/blacktear23/dragonbolt/mvcc"
	"github.com/blacktear23/dragonbolt/protocol"
	"github.com/blacktear23/dragonbolt/txn"
)

const (
	ISO_LEVEL_RC = 1
	ISO_LEVEL_RR = 2
)

var (
	_ txn.KVOperation = (*mvccTxnOps)(nil)

	ErrKeyLocked   = errors.New("Key is locked")
	ErrWrongResult = errors.New("Wrong result")
	ErrTxnConflict = errors.New("Transaction conflict")
)

type mvccTxnOps struct {
	rc         *rclient
	isoLevel   int
	txnVer     uint64
	readTxnVer uint64
}

func (o *mvccTxnOps) Batch(muts []kv.Mutation) error {
	data, err := json.Marshal(muts)
	if err != nil {
		return nil
	}
	ret, err := o.rc.trySyncPropose(data, 100)
	switch ret.Value {
	case kv.RESULT_KEY_LOCKED:
		return ErrKeyLocked
	case kv.RESULT_TXN_CONFLICT:
		return ErrTxnConflict
	}
	return err
}

func (o *mvccTxnOps) Get(key []byte) ([]byte, error) {
	query := &kv.Query{
		Op:      kv.MVCC_GET_VALUE,
		Keys:    [][]byte{key},
		Version: o.readTxnVer,
	}
	result, err := o.rc.trySyncRead(query, 100)
	if err != nil {
		return nil, err
	}
	if len(result.KVS) == 0 {
		return nil, nil
	}
	return result.KVS[0].Value, nil
}

func (o *mvccTxnOps) BatchGet(keys [][]byte) ([]kv.KVPair, error) {
	query := &kv.Query{
		Op:      kv.MVCC_GET_VALUE,
		Keys:    keys,
		Version: o.readTxnVer,
	}
	result, err := o.rc.trySyncRead(query, 100)
	if err != nil {
		return nil, err
	}
	return result.KVS, nil
}

func (o *mvccTxnOps) Scan(start []byte, end []byte, limit int) ([]kv.KVPair, error) {
	query := &kv.Query{
		Op:      kv.MVCC_SCAN,
		Start:   start,
		End:     end,
		Limit:   limit,
		Version: o.readTxnVer,
	}
	result, err := o.rc.trySyncRead(query, 100)
	if err != nil {
		return nil, err
	}
	return result.KVS, nil
}

func (o *mvccTxnOps) LockKey(key []byte) error {
	muts := []kv.Mutation{
		kv.Mutation{
			Op:      kv.MVCC_LOCK,
			Key:     key,
			Version: o.txnVer,
		},
	}
	data, err := json.Marshal(muts)
	if err != nil {
		return err
	}
	resp, err := o.rc.trySyncPropose(data, 100)
	if err != nil {
		return err
	}
	switch resp.Value {
	case kv.RESULT_OK:
		return nil
	case kv.RESULT_KEY_LOCKED:
		return ErrKeyLocked
	default:
		return ErrWrongResult
	}
}

func (o *mvccTxnOps) UnlockKey(key []byte) error {
	muts := []kv.Mutation{
		kv.Mutation{
			Op:      kv.MVCC_UNLOCK,
			Key:     key,
			Version: o.txnVer,
		},
	}
	data, err := json.Marshal(muts)
	if err != nil {
		return err
	}
	resp, err := o.rc.trySyncPropose(data, 100)
	if err != nil {
		return err
	}
	switch resp.Value {
	case kv.RESULT_OK:
		return nil
	case kv.RESULT_KEY_LOCKED:
		return ErrKeyLocked
	default:
		return ErrWrongResult
	}
}

func (c *rclient) newMvccTxnOps(isoLevel int, txnVer uint64) *mvccTxnOps {
	readTxnVer := txnVer
	if isoLevel == ISO_LEVEL_RC {
		readTxnVer = mvcc.MAX_UINT64
	}
	return &mvccTxnOps{
		rc:         c,
		isoLevel:   isoLevel,
		txnVer:     txnVer,
		readTxnVer: readTxnVer,
	}
}

func (c *rclient) handleBegin(args []protocol.Encodable) protocol.Encodable {
	if c.txn != nil {
		return protocol.NewSimpleErrorf("Already begin")
	}
	level := ISO_LEVEL_RR
	if len(args) > 0 {
		levelVal, err := c.parseString(args[0])
		if err != nil {
			return protocol.NewSimpleErrorf(err.Error())
		}
		levelStr := strings.ToUpper(levelVal)
		switch levelStr {
		case "RC":
			level = ISO_LEVEL_RC
		case "RR":
			level = ISO_LEVEL_RR
		}
	}
	txnVer, err := c.getTso()
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	c.txnVer = txnVer
	txn := txn.NewMemMergeTxn(c.newMvccTxnOps(level, c.txnVer), kv.MVCC_SET, kv.MVCC_DEL, c.txnVer)
	err = txn.Begin()
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	c.txn = txn
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleCommit(args []protocol.Encodable) protocol.Encodable {
	if c.txn == nil {
		return protocol.NewSimpleError("Transaction not begin")
	}
	commitVer, err := c.getTso()
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	err = c.txn.Commit(commitVer)
	c.txn = nil
	c.txnVer = 0
	if err != nil {
		if err == ErrKeyLocked || err == ErrTxnConflict {
			return protocol.NewSimpleError(err.Error())
		}
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleRollback(args []protocol.Encodable) protocol.Encodable {
	if c.txn == nil {
		return protocol.NewSimpleError("Transaction not begin")
	}
	err := c.txn.Rollback()
	c.txn = nil
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	return protocol.NewSimpleString("OK")
}
