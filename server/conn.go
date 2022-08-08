package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/blacktear23/dragonbolt/kv"
	"github.com/blacktear23/dragonbolt/protocol"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

type rclient struct {
	conn net.Conn
	rs   *RedisServer
}

func (c *rclient) resp(resp protocol.Encodable) error {
	data := resp.Encode()
	_, err := c.conn.Write(data)
	return err
}

func (c *rclient) parseKey(arg protocol.Encodable) ([]byte, error) {
	switch val := arg.(type) {
	case *protocol.SimpleString:
		return val.Bytes(), nil
	case *protocol.BlobString:
		return val.Bytes(), nil
	default:
		return nil, errors.New("Invalid key")
	}
}

func (d *rclient) parseNumber(arg protocol.Encodable) (int64, error) {
	switch val := arg.(type) {
	case *protocol.Number:
		return val.GetNumber(), nil
	case *protocol.SimpleString:
		return strconv.ParseInt(val.String(), 10, 64)
	case *protocol.BlobString:
		return strconv.ParseInt(val.String(), 10, 64)
	default:
		return 0, errors.New("Invalid number")
	}
}

func (c *rclient) handleRequests(buf []byte) error {
	if len(buf) > 4 {
		if bytes.Equal(buf[0:4], []byte("PING")) {
			err := c.resp(c.handleCommand("ping", nil))
			if err != nil {
				return err
			}
			buf = buf[4:]
		}
	}
	parser := protocol.NewParser(buf)
	for {
		if parser.AllParsed() {
			return nil
		}
		request, err := parser.Next()
		if err != nil {
			log.Println("[Error] Request parse error:", err)
			return err
		}
		req, ok := request.(protocol.Array)
		if !ok {
			log.Println("[Error] Invalid request data")
			return c.resp(protocol.NewSimpleError("Invalid request"))
		}
		if len(req) < 1 {
			log.Println("[Error] Invalid request data")
			return c.resp(protocol.NewSimpleError("Invalid request"))
		}
		cmd, err := c.getCommand(req[0])
		if err != nil {
			return c.resp(protocol.NewSimpleError("Invalid request"))
		}
		respData := c.handleCommand(cmd, req[1:])
		err = c.resp(respData)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *rclient) processInc(key []byte, delta int64) (bool, int64, error) {
	query := &kv.Query{
		Op:  kv.GET_VALUE,
		Key: key,
	}
	result, err := c.rs.trySyncRead(query, 100)
	if err != nil {
		return false, 0, err
	}
	var (
		originNumber int64  = 0
		oldValue     []byte = nil
		newValue     []byte = nil
	)
	kvs := result.KVS
	if len(kvs) > 0 {
		originNumber, err = strconv.ParseInt(string(kvs[0].Value), 10, 64)
		if err != nil {
			return false, 0, errors.New("Value not a number")
		}
		oldValue = kvs[0].Value
	}
	originNumber += delta
	newValue = []byte(strconv.FormatInt(originNumber, 10))
	resp, err := c.handleCas(key, oldValue, newValue)
	if err != nil {
		return false, 0, err
	}
	return resp.Value == kv.RESULT_OK, originNumber, nil
}

func (c *rclient) handleCas(key []byte, value []byte, newValue []byte) (sm.Result, error) {
	mut := &kv.Mutation{
		Op:        kv.CAS,
		Keys:      [][]byte{key},
		Values:    [][]byte{value},
		NewValues: [][]byte{newValue},
	}
	data, err := json.Marshal(mut)
	if err != nil {
		return sm.Result{}, err
	}
	return c.rs.trySyncPropose(data, 100)
}

func (c *rclient) getCommand(cmd protocol.Encodable) (string, error) {
	switch obj := cmd.(type) {
	case *protocol.SimpleString:
		return strings.ToLower(obj.String()), nil
	case *protocol.BlobString:
		return strings.ToLower(obj.String()), nil
	default:
		return "", ErrInvalidCommand
	}
}

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
		Op:  kv.GET_VALUE,
		Key: key,
	}
	result, err := c.rs.trySyncRead(query, 100)
	if err != nil {
		log.Println("[ERR]", err)
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	kvs := result.KVS
	if len(kvs) == 0 {
		return protocol.NewNull()
	}
	return protocol.NewBlobString(kvs[0].Value)
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
	kv := &kv.Mutation{
		Op:     kv.PUT,
		Keys:   [][]byte{key},
		Values: [][]byte{val.Bytes()},
	}
	data, err := json.Marshal(kv)
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
	kv := &kv.Mutation{
		Op:     kv.DEL,
		Keys:   [][]byte{key},
		Values: nil,
	}
	data, err := json.Marshal(kv)
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

// ~ Command Handlers
