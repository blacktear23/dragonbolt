package server

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/blacktear23/dragonbolt/kv"
	"github.com/blacktear23/dragonbolt/protocol"
	"github.com/blacktear23/dragonbolt/txn"
)

var (
	_ txn.KVOperation = (txn.KVOperation)(&txnOps{})
)

type txnOps struct {
	rc *rclient
}

func (o *txnOps) Batch(muts []kv.Mutation) error {
	data, err := json.Marshal(muts)
	if err != nil {
		return err
	}
	_, err = o.rc.trySyncPropose(data, 100)
	return err
}

func (o *txnOps) Get(key []byte) ([]byte, error) {
	query := &kv.Query{
		Op:   kv.GET_VALUE,
		Keys: [][]byte{key},
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

func (o *txnOps) Scan(start []byte, end []byte, limit int) ([]kv.KVPair, error) {
	query := &kv.Query{
		Op:      kv.SCAN,
		Start:   start,
		End:     end,
		Limit:   limit,
		SameLen: true,
	}
	result, err := o.rc.trySyncRead(query, 100)
	if err != nil {
		return nil, err
	}
	return result.KVS, nil
}

func (c *rclient) newTxnOps() *txnOps {
	return &txnOps{
		rc: c,
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

func (c *rclient) handleTxnScan(args []protocol.Encodable) protocol.Encodable {
	if c.txn == nil {
		return protocol.NewSimpleError("Transaction not begin")
	}
	scanHelp := "TSCAN StartKey [EndKey] [LIMIT lim]"
	if len(args) < 1 {
		return protocol.NewSimpleErrorf("Invalid start key parameters, %s", scanHelp)
	}
	startKey, err := c.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	var (
		endKey []byte = nil
		limit  int64  = 1000
	)
	if len(args) == 2 {
		endKey, err = c.parseKey(args[1])
		if err != nil {
			return protocol.NewSimpleErrorf("Invalid end key parameters, %s", scanHelp)
		}
	} else if len(args) == 3 {
		kw, err := c.parseKey(args[1])
		if err != nil {
			return protocol.NewSimpleErrorf("Invalid limit parameters, %s", scanHelp)
		}
		if strings.ToUpper(string(kw)) != "LIMIT" {
			return protocol.NewSimpleErrorf("Invalid limit parameters, %s", scanHelp)
		}

		limit, err = c.parseNumber(args[2])
		if err != nil {
			return protocol.NewSimpleErrorf("Invalid limit parameters, %s", scanHelp)
		}
	} else if len(args) == 4 {
		endKey, err = c.parseKey(args[1])
		if err != nil {
			return protocol.NewSimpleErrorf("Invalid end key parameters, %s", scanHelp)
		}
		kw, err := c.parseKey(args[2])
		if err != nil || strings.ToUpper(string(kw)) != "LIMIT" {
			return protocol.NewSimpleErrorf("Invalid limit parameters, %s", scanHelp)
		}
		limit, err = c.parseNumber(args[3])
		if err != nil {
			return protocol.NewSimpleErrorf("Invalid limit parameters, %s", scanHelp)
		}
	}
	iter, err := c.txn.Cursor()
	if err != nil {
		return protocol.NewSimpleErrorf("Internal error: %v", err)
	}
	err = iter.Seek(startKey)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal error: %v", err)
	}
	ret := protocol.Array{}
	for i := int64(0); i < limit; i++ {
		key, _, err := iter.Next()
		if err != nil {
			return protocol.NewSimpleErrorf("Internal error: %v", err)
		}
		if key == nil {
			break
		}
		if keyCompare(key, endKey) >= 0 {
			break
		}
		ret = append(ret, protocol.NewBlobString(key))
	}
	return ret
}

func keyCompare(val1 []byte, val2 []byte) int {
	if val2 == nil {
		return -1
	}
	return bytes.Compare(val1, val2)
}
