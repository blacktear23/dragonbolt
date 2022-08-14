package server

import (
	"encoding/json"
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
)

type mvccTxnOps struct {
	rc         *rclient
	isoLevel   int
	readTxnVer uint64
}

func (o *mvccTxnOps) Batch(muts []kv.Mutation) error {
	data, err := json.Marshal(muts)
	if err != nil {
		return nil
	}
	_, err = o.rc.trySyncPropose(data, 100)
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

func (c *rclient) newMvccTxnOps(isoLevel int, txnVer uint64) *mvccTxnOps {
	if isoLevel == ISO_LEVEL_RC {
		txnVer = mvcc.MAX_UINT64
	}
	return &mvccTxnOps{
		rc:         c,
		isoLevel:   isoLevel,
		readTxnVer: txnVer,
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
	err := c.txn.Commit()
	c.txn = nil
	c.txnVer = 0
	if err != nil {
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
