package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/blacktear23/dragonbolt/kv"
	"github.com/blacktear23/dragonbolt/protocol"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

var (
	ErrInvalidCommand = errors.New("Invalid Command")
)

type RedisServer struct {
	shardID  uint64
	addr     string
	timeout  time.Duration
	nodeHost *dragonboat.NodeHost
	cs       *client.Session
	ln       net.Listener
}

func NewRedisServer(addr string, nh *dragonboat.NodeHost, sid uint64) *RedisServer {
	cs := nh.GetNoOPSession(sid)
	return &RedisServer{
		shardID:  sid,
		addr:     addr,
		nodeHost: nh,
		timeout:  10 * time.Second,
		cs:       cs,
	}
}

func (rs *RedisServer) Run() error {
	return rs.run()
}

func (rs *RedisServer) run() error {
	ln, err := net.Listen("tcp", rs.addr)
	if err != nil {
		return err
	}
	rs.ln = ln
	go rs.runListen(ln)
	return nil
}

func (rs *RedisServer) runListen(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Listen got error:", err)
			continue
		}
		go rs.handleConn(conn)
	}
}

func (rs *RedisServer) handleConn(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 16384)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Println("Read data got error:", err)
			return
		}
		err = rs.handleRequests(buf[0:n], conn)
		if err != nil {
			log.Println("Error:", err)
			return
		}
	}
}

func (rs *RedisServer) resp(resp protocol.Encodable, conn net.Conn) error {
	data := resp.Encode()
	_, err := conn.Write(data)
	return err
}

func (rs *RedisServer) handleRequests(buf []byte, conn net.Conn) error {
	if len(buf) > 4 {
		if bytes.Equal(buf[0:4], []byte("PING")) {
			err := rs.resp(rs.handleCommand("ping", nil), conn)
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
			return rs.resp(protocol.NewSimpleError("Invalid request"), conn)
		}
		if len(req) < 1 {
			log.Println("[Error] Invalid request data")
			return rs.resp(protocol.NewSimpleError("Invalid request"), conn)
		}
		cmd, err := rs.getCommand(req[0])
		if err != nil {
			return rs.resp(protocol.NewSimpleError("Invalid request"), conn)
		}
		respData := rs.handleCommand(cmd, req[1:])
		err = rs.resp(respData, conn)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *RedisServer) getCommand(cmd protocol.Encodable) (string, error) {
	switch obj := cmd.(type) {
	case *protocol.SimpleString:
		return strings.ToLower(obj.String()), nil
	case *protocol.BlobString:
		return strings.ToLower(obj.String()), nil
	default:
		return "", ErrInvalidCommand
	}
}

func (rs *RedisServer) handleCommand(cmd string, args []protocol.Encodable) protocol.Encodable {
	switch cmd {
	case "command":
		return rs.handleCmd(args)
	case "get":
		return rs.handleGet(args)
	case "set":
		return rs.handleSet(args)
	case "del":
		return rs.handleDel(args)
	case "ping":
		return protocol.NewSimpleString("PONG")
	case "config":
		return protocol.NewSimpleString("OK")
	case "inc":
		return rs.handleIncDec(args, 1)
	case "dec":
		return rs.handleIncDec(args, -1)
	default:
		return protocol.NewSimpleErrorf("Unsupport command: %s", cmd)
	}
}

func (rs *RedisServer) handleCmd(args []protocol.Encodable) protocol.Encodable {
	return protocol.NewSimpleString("OK")
}

func (rs *RedisServer) handleGet(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 1 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := rs.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	ctx, cancel := context.WithTimeout(context.Background(), rs.timeout)
	defer cancel()
	result, err := rs.nodeHost.SyncRead(ctx, rs.shardID, key)
	if err != nil {
		log.Println("[ERR]", err)
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	rdata := result.([]byte)
	if len(rdata) == 0 {
		return protocol.NewNull()
	}
	return protocol.NewBlobString(rdata)
}

func (rs *RedisServer) parseKey(arg protocol.Encodable) ([]byte, error) {
	switch val := arg.(type) {
	case *protocol.SimpleString:
		return val.Bytes(), nil
	case *protocol.BlobString:
		return val.Bytes(), nil
	default:
		return nil, errors.New("Invalid key")
	}
}

func (rs *RedisServer) handleSet(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 2 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := rs.parseKey(args[0])
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
	_, err = rs.trySyncPropose(data, 10)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	return protocol.NewSimpleString("OK")
}

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func (rs *RedisServer) trySyncPropose(data []byte, tryTimes int) (sm.Result, error) {
	var (
		result sm.Result
		err    error
	)
	for i := 0; i < tryTimes; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), rs.timeout)
		result, err = rs.nodeHost.SyncPropose(ctx, rs.cs, data)
		cancel()
		if err == nil {
			return result, nil
		}
		// Err is not nil
		if err == dragonboat.ErrShardNotReady {
			// Shard not ready just retry
			err = nil
			dur := min(i+1, 10)
			time.Sleep(time.Duration(dur) * time.Millisecond)
			continue
		} else {
			return result, err
		}
	}
	return result, err
}

func (rs *RedisServer) handleDel(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 1 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := rs.parseKey(args[0])
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
	_, err = rs.trySyncPropose(data, 10)
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	return protocol.NewSimpleString("OK")
}

func (rs *RedisServer) handleIncDec(args []protocol.Encodable, delta int64) protocol.Encodable {
	if len(args) < 1 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := rs.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	var (
		updated   bool
		newNumber int64
	)
	for {
		updated, newNumber, err = rs.processInc(key, delta)
		if err != nil {
			return protocol.NewSimpleError(err.Error())
		}
		if updated {
			break
		}
	}
	return protocol.NewNumber(newNumber)
}

func (rs *RedisServer) processInc(key []byte, delta int64) (bool, int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), rs.timeout)
	defer cancel()
	result, err := rs.nodeHost.SyncRead(ctx, rs.shardID, key)
	if err != nil {
		return false, 0, err
	}
	var (
		originNumber int64  = 0
		oldValue     []byte = nil
		newValue     []byte = nil
	)
	rdata := result.([]byte)
	if len(rdata) > 0 {
		originNumber, err = strconv.ParseInt(string(rdata), 10, 64)
		if err != nil {
			return false, 0, errors.New("Value not a number")
		}
		oldValue = rdata
	}
	originNumber += delta
	newValue = []byte(strconv.FormatInt(originNumber, 10))
	resp, err := rs.handleCas(key, oldValue, newValue)
	if err != nil {
		return false, 0, err
	}
	return resp.Value == kv.RESULT_OK, originNumber, nil
}

func (rs *RedisServer) handleCas(key []byte, value []byte, newValue []byte) (sm.Result, error) {
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
	return rs.trySyncPropose(data, 10)
}

func (rs *RedisServer) Close() error {
	if rs.ln != nil {
		return rs.ln.Close()
	}
	return nil
}
