package server

import (
	"strings"

	"github.com/blacktear23/dragonbolt/kv"
	"github.com/blacktear23/dragonbolt/protocol"
)

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

func (c *rclient) handleTxnMset(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 2 || len(args)%2 != 0 {
		return protocol.NewSimpleError("Need more arguments")
	}
	kvs := []kv.KVPair{}
	for i := 0; i < len(args); i += 2 {
		key, err := c.parseKey(args[i])
		if err != nil {
			return protocol.NewSimpleError(err.Error())
		}
		val, ok := args[i+1].(protocol.Savable)
		if !ok {
			return protocol.NewSimpleError("Invalid data")
		}
		kvs = append(kvs, kv.KVPair{
			Key:   key,
			Value: val.Bytes(),
		})
	}
	for _, kv := range kvs {
		c.txn.Set(kv.Key, kv.Value)
	}
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleTxnMget(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 1 {
		return protocol.NewSimpleError("Need more arguments")
	}
	keys := make([][]byte, 0, len(args))
	for _, arg := range args {
		key, err := c.parseKey(arg)
		if err != nil {
			return protocol.NewSimpleError(err.Error())
		}
		keys = append(keys, key)
	}
	ret := protocol.Array{}
	for _, key := range keys {
		value, err := c.txn.Get(key)
		if err != nil {
			return protocol.NewSimpleErrorf("Internal Error: %v", err)
		}
		if value == nil {
			ret = append(ret, protocol.NewNull())
		} else {
			ret = append(ret, protocol.NewBlobString(value))
		}
	}
	return ret
}
