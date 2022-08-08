package server

import (
	"context"
	"errors"
	"log"
	"net"
	"time"

	"github.com/blacktear23/dragonbolt/kv"
	"github.com/blacktear23/dragonbolt/tso"
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
	tsoSrv   *tso.TSOServer
}

func NewRedisServer(addr string, nh *dragonboat.NodeHost, sid uint64, tsoServer *tso.TSOServer) *RedisServer {
	cs := nh.GetNoOPSession(sid)
	return &RedisServer{
		shardID:  sid,
		addr:     addr,
		nodeHost: nh,
		timeout:  10 * time.Second,
		cs:       cs,
		tsoSrv:   tsoServer,
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
	c := rclient{
		conn: conn,
		rs:   rs,
	}
	buf := make([]byte, 16384)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Println("Read data got error:", err)
			return
		}
		err = c.handleRequests(buf[0:n])
		if err != nil {
			log.Println("Error:", err)
			return
		}
	}
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
			dur := min(1<<i, 500)
			time.Sleep(time.Duration(dur) * time.Millisecond)
			continue
		} else {
			return result, err
		}
	}
	return result, err
}

func (rs *RedisServer) trySyncRead(query *kv.Query, tryTimes int) (*kv.QueryResult, error) {
	var (
		err    error
		result interface{}
	)
	for i := 0; i < tryTimes; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), rs.timeout)
		result, err = rs.nodeHost.SyncRead(ctx, rs.shardID, query)
		cancel()
		if err == nil {
			return result.(*kv.QueryResult), nil
		}
		// Err is not nil
		if err == dragonboat.ErrShardNotReady {
			dur := min(1<<i, 500)
			time.Sleep(time.Duration(dur) * time.Millisecond)
			continue
		} else {
			return nil, err
		}
	}
	if result == nil {
		return nil, err
	}
	return result.(*kv.QueryResult), err
}

func (rs *RedisServer) Close() error {
	if rs.ln != nil {
		return rs.ln.Close()
	}
	return nil
}
