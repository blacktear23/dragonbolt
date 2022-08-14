package server

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/blacktear23/dragonbolt/kv"
	"github.com/blacktear23/dragonbolt/protocol"
)

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
	_, err = c.trySyncPropose(data, 100)
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
	result, err := c.trySyncRead(query, 100)
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
	_, err = c.trySyncPropose(data, 100)
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
	result, err := c.trySyncRead(query, 100)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	ret := protocol.Array{}
	for _, kvp := range result.KVS {
		ret = append(ret, protocol.NewBlobString(kvp.Key))
	}
	return ret
}
