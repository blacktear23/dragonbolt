package server

import (
	"encoding/json"

	"github.com/blacktear23/dragonbolt/kv"
	"github.com/blacktear23/dragonbolt/protocol"
	"github.com/blacktear23/dragonbolt/txn"
)

var (
	_ txn.KVOperation = (txn.KVOperation)(&txnOps{})
)

type txnOps struct {
	rs *RedisServer
}

func (o *txnOps) Batch(muts []kv.Mutation) error {
	data, err := json.Marshal(muts)
	if err != nil {
		return err
	}
	_, err = o.rs.trySyncPropose(data, 100)
	return err
}

func (o *txnOps) Get(key []byte) ([]byte, error) {
	query := &kv.Query{
		Op:   kv.GET_VALUE,
		Keys: [][]byte{key},
	}
	result, err := o.rs.trySyncRead(query, 100)
	if err != nil {
		return nil, err
	}
	if len(result.KVS) == 0 {
		return nil, nil
	}
	return result.KVS[0].Value, nil
}

func (o *txnOps) Scan(start []byte, end []byte, limit int) ([]kv.KVPair, error) {
	query := &kv.Query{
		Op:      kv.SCAN,
		Start:   start,
		End:     end,
		Limit:   limit,
		SameLen: true,
	}
	result, err := o.rs.trySyncRead(query, 100)
	if err != nil {
		return nil, err
	}
	return result.KVS, nil
}

func (c *rclient) newTxnOps() *txnOps {
	return &txnOps{
		rs: c.rs,
	}
}

func (c *rclient) handleBegin(args []protocol.Encodable) protocol.Encodable {
	if c.txn != nil {
		return protocol.NewSimpleError("Already begin")
	}
	txn := txn.NewRcTxn(c.newTxnOps())
	err := txn.Begin()
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	c.txn = txn
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleTxnSet(args []protocol.Encodable) protocol.Encodable {
	if c.txn == nil {
		return protocol.NewSimpleError("Transaction not begin")
	}
	if len(args) < 2 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := c.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	val, ok := args[1].(protocol.Savable)
	if !ok {
		return protocol.NewSimpleError("Invalid data")
	}
	err = c.txn.Set(key, val.Bytes())
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleTxnGet(args []protocol.Encodable) protocol.Encodable {
	if c.txn == nil {
		return protocol.NewSimpleError("Transaction not begin")
	}
	if len(args) < 1 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := c.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	value, err := c.txn.Get(key)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	if value == nil {
		return protocol.NewNull()
	}
	return protocol.NewBlobString(value)
}

func (c *rclient) handleTxnDelete(args []protocol.Encodable) protocol.Encodable {
	if c.txn == nil {
		return protocol.NewSimpleError("Transaction not begin")
	}
	if len(args) < 1 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := c.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	err = c.txn.Delete(key)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleCommit(args []protocol.Encodable) protocol.Encodable {
	if c.txn == nil {
		return protocol.NewSimpleError("Transaction not begin")
	}
	err := c.txn.Commit()
	c.txn = nil
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
