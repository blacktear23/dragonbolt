package server

import (
	"errors"
	"strconv"
	"strings"

	"github.com/blacktear23/dragonbolt/kv"
	"github.com/blacktear23/dragonbolt/protocol"
	"github.com/blacktear23/dragonbolt/txn"
)

func (c *rclient) autoBegin() (bool, error) {
	if c.txn == nil {
		level := ISO_LEVEL_RR
		txnVer, err := c.getTso()
		if err != nil {
			return false, err
		}
		c.txnVer = txnVer
		txn := txn.NewMemMergeTxn(c.newMvccTxnOps(level, c.txnVer), kv.MVCC_SET, kv.MVCC_DEL, c.txnVer)
		err = txn.Begin()
		if err != nil {
			return false, err
		}
		c.txn = txn
		return true, nil
	}
	return false, nil
}

func (c *rclient) autoCommit() error {
	if c.txn != nil {
		commitVer, err := c.getTso()
		if err != nil {
			return err
		}
		c.txn.Commit(commitVer)
		c.txn = nil
		c.txnVer = 0
	}
	return nil
}

func (c *rclient) autoRollback() error {
	if c.txn != nil {
		err := c.txn.Rollback()
		c.txn = nil
		c.txnVer = 0
		return err
	}
	return nil
}

func (c *rclient) handleTxnSet(args []protocol.Encodable) protocol.Encodable {
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
	autoCommit, err := c.autoBegin()
	if err != nil {
		return protocol.NewSimpleErrorf("Transaction Error: %v", err)
	}
	err = c.txn.Set(key, val.Bytes())
	if err != nil {
		if autoCommit {
			c.autoRollback()
		}
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	if autoCommit {
		err := c.autoCommit()
		if err != nil {
			return protocol.NewSimpleErrorf("Transaction Error: %v", err)
		}
	}
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleTxnGet(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 1 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := c.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}

	autoCommit, err := c.autoBegin()
	if err != nil {
		return protocol.NewSimpleErrorf("Transaction Error: %v", err)
	}
	value, err := c.txn.Get(key)
	if err != nil {
		if autoCommit {
			c.autoRollback()
		}
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	if autoCommit {
		err = c.autoCommit()
		if err != nil {
			return protocol.NewSimpleErrorf("Transaction Error: %v", err)
		}
	}
	if value == nil {
		return protocol.NewNull()
	}
	return protocol.NewBlobString(value)
}

func (c *rclient) handleTxnDelete(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 1 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := c.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}

	autoCommit, err := c.autoBegin()
	if err != nil {
		return protocol.NewSimpleErrorf("Transaction Error: %v", err)
	}
	err = c.txn.Delete(key)
	if err != nil {
		if autoCommit {
			c.autoRollback()
		}
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	if autoCommit {
		err = c.autoCommit()
		if err != nil {
			return protocol.NewSimpleErrorf("Transaction Error: %v", err)
		}
	}
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleTxnScan(args []protocol.Encodable) protocol.Encodable {
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
	autoCommit, err := c.autoBegin()
	if err != nil {
		return protocol.NewSimpleErrorf("Transaction Error: %v", err)
	}
	iter, err := c.txn.Cursor()
	if err != nil {
		if autoCommit {
			c.autoRollback()
		}
		return protocol.NewSimpleErrorf("Internal error: %v", err)
	}
	err = iter.Seek(startKey)
	if err != nil {
		if autoCommit {
			c.autoRollback()
		}
		return protocol.NewSimpleErrorf("Internal error: %v", err)
	}
	ret := protocol.Array{}
	for i := int64(0); i < limit; i++ {
		key, _, err := iter.Next()
		if err != nil {
			if autoCommit {
				c.autoRollback()
			}
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
	if autoCommit {
		err = c.autoCommit()
		if err != nil {
			return protocol.NewSimpleErrorf("Transaction Error: %v", err)
		}
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
	autoCommit, err := c.autoBegin()
	if err != nil {
		return protocol.NewSimpleErrorf("Transaction Error: %v", err)
	}
	for _, kv := range kvs {
		c.txn.Set(kv.Key, kv.Value)
	}
	if autoCommit {
		err = c.autoCommit()
		if err != nil {
			return protocol.NewSimpleErrorf("Transaction Error: %v", err)
		}
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
	autoCommit, err := c.autoBegin()
	if err != nil {
		return protocol.NewSimpleErrorf("Transaction Error: %v", err)
	}
	ret := protocol.Array{}
	for _, key := range keys {
		value, err := c.txn.Get(key)
		if err != nil {
			if autoCommit {
				c.autoRollback()
			}
			return protocol.NewSimpleErrorf("Internal Error: %v", err)
		}
		if value == nil {
			ret = append(ret, protocol.NewNull())
		} else {
			ret = append(ret, protocol.NewBlobString(value))
		}
	}
	if autoCommit {
		err = c.autoCommit()
		if err != nil {
			return protocol.NewSimpleErrorf("Transaction Error: %v", err)
		}
	}
	return ret
}

func (c *rclient) handleTxnLock(args []protocol.Encodable) protocol.Encodable {
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
	err = c.txn.LockKey(key)
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleTxnUnlock(args []protocol.Encodable) protocol.Encodable {
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
	err = c.txn.UnlockKey(key)
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleTxnIncDec(args []protocol.Encodable, delta int64) protocol.Encodable {
	if len(args) < 1 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := c.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	autoCommit, err := c.autoBegin()
	if err != nil {
		return protocol.NewSimpleErrorf("Transaction Error: %v", err)
	}
	newVal, err := c.processTxnInc(key, delta)
	if err != nil {
		if autoCommit {
			c.autoRollback()
		}
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	if autoCommit {
		err = c.autoCommit()
		if err != nil {
			return protocol.NewSimpleErrorf("Transaction Error: %v", err)
		}
	}
	return protocol.NewNumber(newVal)
}

func (c *rclient) processTxnInc(key []byte, delta int64) (int64, error) {
	value, err := c.txn.Get(key)
	if err != nil {
		return 0, err
	}
	var (
		originNumber int64 = 0
	)
	if value != nil {
		originNumber, err = strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return 0, errors.New("Value not a number")
		}
	}
	newVal := originNumber + delta
	newValue := []byte(strconv.FormatInt(newVal, 10))
	err = c.txn.Set(key, newValue)
	if err != nil {
		return 0, err
	}
	return newVal, nil
}
