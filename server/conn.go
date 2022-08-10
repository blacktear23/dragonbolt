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
	"github.com/blacktear23/dragonbolt/txn"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

type rclient struct {
	conn net.Conn
	rs   *RedisServer
	txn  txn.Txn
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

func (c *rclient) parseCf(arg protocol.Encodable) (string, error) {
	var (
		ret string
		err error
	)

	switch val := arg.(type) {
	case *protocol.SimpleString:
		ret = val.String()
	case *protocol.BlobString:
		ret = val.String()
	default:
		err = errors.New("Invalid CF")
	}
	if err != nil {
		return ret, err
	}
	err = kv.CheckColumnFamily(ret)
	return ret, err
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
		Op:   kv.GET_VALUE,
		Keys: [][]byte{key},
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
	if len(kvs) > 0 && kvs[0].Value != nil {
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
	mut := []kv.Mutation{
		kv.Mutation{
			Op:       kv.CAS,
			Key:      key,
			Value:    value,
			NewValue: newValue,
		},
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
