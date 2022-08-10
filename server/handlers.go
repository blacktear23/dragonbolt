package server

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/blacktear23/dragonbolt/kv"
	"github.com/blacktear23/dragonbolt/protocol"
)

func (c *rclient) handleCommand(cmd string, args []protocol.Encodable) protocol.Encodable {
	switch cmd {
	case "command":
		return c.handleCmd(args)
	case "get":
		return c.handleGet(args)
	case "set":
		return c.handleSet(args)
	case "del":
		return c.handleDel(args)
	case "ping":
		return protocol.NewSimpleString("PONG")
	case "config":
		return protocol.NewSimpleString("OK")
	case "inc":
		return c.handleIncDec(args, 1)
	case "dec":
		return c.handleIncDec(args, -1)
	case "tso":
		return c.handleTSO(args)
	case "scan":
		return c.handleScan(args)
	case "mget":
		return c.handleMget(args)
	case "mset":
		return c.handleMset(args)
	case "cfset":
		return c.handleCfSet(args)
	case "cfget":
		return c.handleCfGet(args)
	case "cfdel":
		return c.handleCfDel(args)
	case "cfscan":
		return c.handleCfScan(args, false)
	case "cfrscan":
		return c.handleCfScan(args, true)
	case "begin":
		return c.handleBegin(args)
	case "tset":
		return c.handleTxnSet(args)
	case "tget":
		return c.handleTxnGet(args)
	case "tdel":
		return c.handleTxnDelete(args)
	case "commit":
		return c.handleCommit(args)
	case "rollback":
		return c.handleRollback(args)
	default:
		return protocol.NewSimpleErrorf("Unsupport command: %s", cmd)
	}
}

// Command handlers

func (c *rclient) handleCmd(args []protocol.Encodable) protocol.Encodable {
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleGet(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 1 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := c.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	query := &kv.Query{
		Op:   kv.GET_VALUE,
		Keys: [][]byte{key},
	}
	result, err := c.rs.trySyncRead(query, 100)
	if err != nil {
		log.Println("[ERR]", err)
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	kvs := result.KVS
	if len(kvs) == 0 || kvs[0].Value == nil {
		return protocol.NewNull()
	}
	return protocol.NewBlobString(kvs[0].Value)
}

func (c *rclient) handleMget(args []protocol.Encodable) protocol.Encodable {
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
	query := &kv.Query{
		Op:   kv.GET_VALUE,
		Keys: keys,
	}
	result, err := c.rs.trySyncRead(query, 100)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	ret := protocol.Array{}
	for _, kvp := range result.KVS {
		if kvp.Value != nil {
			ret = append(ret, protocol.NewBlobString(kvp.Value))
		} else {
			ret = append(ret, protocol.NewNull())
		}
	}
	return ret
}

func (c *rclient) handleMset(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 2 || len(args)%2 != 0 {
		return protocol.NewSimpleError("Need more arguments")
	}
	muts := []kv.Mutation{}
	for i := 0; i < len(args); i += 2 {
		key, err := c.parseKey(args[i])
		if err != nil {
			return protocol.NewSimpleError(err.Error())
		}
		val, ok := args[i+1].(protocol.Savable)
		if !ok {
			return protocol.NewSimpleError("Invalid data")
		}
		muts = append(muts, kv.Mutation{
			Op:    kv.PUT,
			Key:   key,
			Value: val.Bytes(),
		})
	}
	data, err := json.Marshal(muts)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	_, err = c.rs.trySyncPropose(data, 100)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleSet(args []protocol.Encodable) protocol.Encodable {
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
	muts := []kv.Mutation{
		kv.Mutation{
			Op:    kv.PUT,
			Key:   key,
			Value: val.Bytes(),
		},
	}
	data, err := json.Marshal(muts)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	_, err = c.rs.trySyncPropose(data, 100)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleTSO(args []protocol.Encodable) protocol.Encodable {
	tso, err := c.rs.tsoSrv.GetTSO()
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	return protocol.NewNumberUint(tso)
}

func (c *rclient) handleScan(args []protocol.Encodable) protocol.Encodable {
	scanHelp := "SCAN StartKey [EndKey] [LIMIT lim]"
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

	query := &kv.Query{
		Op:    kv.SCAN_KEY,
		Start: startKey,
		End:   endKey,
		Limit: int(limit),
	}
	result, err := c.rs.trySyncRead(query, 100)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	ret := protocol.Array{}
	for _, kvp := range result.KVS {
		ret = append(ret, protocol.NewBlobString(kvp.Key))
	}
	return ret
}

func (c *rclient) handleDel(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 1 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := c.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	muts := []kv.Mutation{
		kv.Mutation{
			Op:  kv.DEL,
			Key: key,
		},
	}
	data, err := json.Marshal(muts)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	_, err = c.rs.trySyncPropose(data, 100)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleIncDec(args []protocol.Encodable, delta int64) protocol.Encodable {
	if len(args) < 1 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := c.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	var (
		updated   bool
		newNumber int64
	)
	for {
		updated, newNumber, err = c.processInc(key, delta)
		if err != nil {
			return protocol.NewSimpleError(err.Error())
		}
		if updated {
			break
		}
	}
	return protocol.NewNumber(newNumber)
}

func (c *rclient) handleCfSet(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 3 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := c.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	cf, err := c.parseCf(args[1])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	val, ok := args[2].(protocol.Savable)
	if !ok {
		return protocol.NewSimpleError("Invalid data")
	}
	muts := []kv.Mutation{
		kv.Mutation{
			Op:    kv.CF_PUT,
			Cf:    cf,
			Key:   key,
			Value: val.Bytes(),
		},
	}
	data, err := json.Marshal(muts)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	_, err = c.rs.trySyncPropose(data, 100)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleCfGet(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 2 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := c.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	cf, err := c.parseCf(args[1])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	query := &kv.Query{
		Op:   kv.CF_GET,
		Cf:   cf,
		Keys: [][]byte{key},
	}
	result, err := c.rs.trySyncRead(query, 100)
	if err != nil {
		log.Println("[ERR]", err)
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	kvs := result.KVS
	if len(kvs) == 0 || kvs[0].Value == nil {
		return protocol.NewNull()
	}
	return protocol.NewBlobString(kvs[0].Value)
}

func (c *rclient) handleCfDel(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 2 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := c.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	cf, err := c.parseCf(args[1])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	muts := []kv.Mutation{
		kv.Mutation{
			Op:  kv.CF_DEL,
			Cf:  cf,
			Key: key,
		},
	}
	data, err := json.Marshal(muts)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	_, err = c.rs.trySyncPropose(data, 100)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleCfScan(args []protocol.Encodable, reverse bool) protocol.Encodable {
	scanHelp := "SCAN CFName StartKey [EndKey] [LIMIT lim]"
	if len(args) < 2 {
		return protocol.NewSimpleErrorf("Invalid start key parameters, %s", scanHelp)
	}
	cf, err := c.parseCf(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	startKey, err := c.parseKey(args[1])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	var (
		endKey []byte = nil
		limit  int64  = 1000
	)
	if len(args) == 3 {
		endKey, err = c.parseKey(args[2])
		if err != nil {
			return protocol.NewSimpleErrorf("Invalid end key parameters, %s", scanHelp)
		}
	} else if len(args) == 4 {
		kw, err := c.parseKey(args[2])
		if err != nil {
			return protocol.NewSimpleErrorf("Invalid limit parameters, %s", scanHelp)
		}
		if strings.ToUpper(string(kw)) != "LIMIT" {
			return protocol.NewSimpleErrorf("Invalid limit parameters, %s", scanHelp)
		}

		limit, err = c.parseNumber(args[3])
		if err != nil {
			return protocol.NewSimpleErrorf("Invalid limit parameters, %s", scanHelp)
		}
	} else if len(args) == 5 {
		endKey, err = c.parseKey(args[2])
		if err != nil {
			return protocol.NewSimpleErrorf("Invalid end key parameters, %s", scanHelp)
		}
		kw, err := c.parseKey(args[3])
		if err != nil || strings.ToUpper(string(kw)) != "LIMIT" {
			return protocol.NewSimpleErrorf("Invalid limit parameters, %s", scanHelp)
		}
		limit, err = c.parseNumber(args[4])
		if err != nil {
			return protocol.NewSimpleErrorf("Invalid limit parameters, %s", scanHelp)
		}
	}

	op := kv.CF_SCAN_KEY
	if reverse {
		op = kv.CF_RSCAN_KEY
	}
	query := &kv.Query{
		Op:      op,
		Cf:      cf,
		Start:   startKey,
		End:     endKey,
		Limit:   int(limit),
		SameLen: true,
	}
	result, err := c.rs.trySyncRead(query, 100)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	ret := protocol.Array{}
	for _, kvp := range result.KVS {
		ret = append(ret, protocol.NewBlobString(kvp.Key))
	}
	return ret
}

// ~ Command Handlers
