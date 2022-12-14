package server

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"github.com/blacktear23/dragonbolt/kv"
	"github.com/blacktear23/dragonbolt/store"
	"github.com/blacktear23/dragonbolt/tso"
	"github.com/lni/dragonboat/v4"
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
	ln       net.Listener
	tsoSrv   *tso.TSOServer
	sm       *store.StoreManager
	conns    sync.Map
	stop     bool
}

func NewRedisServer(addr string, nh *dragonboat.NodeHost, sid uint64, tsoServer *tso.TSOServer, sm *store.StoreManager) *RedisServer {
	return &RedisServer{
		shardID:  sid,
		addr:     addr,
		nodeHost: nh,
		timeout:  10 * time.Second,
		tsoSrv:   tsoServer,
		sm:       sm,
		conns:    sync.Map{},
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
	for !rs.stop {
		conn, err := ln.Accept()
		if err != nil {
			if rs.stop {
				return
			}
			log.Println("Listen got error:", err)
			continue
		}
		go rs.handleConn(conn)
	}
}

func (rs *RedisServer) handleConn(conn net.Conn) {
	c := &rclient{
		name: conn.RemoteAddr().String(),
		conn: conn,
		rs:   rs,
		sid:  rs.shardID,
	}
	log.Println("New connection:", c.name)
	// Trace connections
	rs.conns.Store(c.name, c)
	// Clean connections
	defer func(c *rclient) {
		c.Close()
		c.rs.conns.Delete(c.name)
	}(c)

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

func (rs *RedisServer) trySyncPropose(shardID uint64, data []byte, tryTimes int) (sm.Result, error) {
	var (
		result sm.Result
		err    error
	)
	cs := rs.nodeHost.GetNoOPSession(shardID)
	for i := 0; i < tryTimes; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), rs.timeout)
		result, err = rs.nodeHost.SyncPropose(ctx, cs, data)
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

func (rs *RedisServer) trySyncRead(shardID uint64, query *kv.Query, tryTimes int) (*kv.QueryResult, error) {
	var (
		err    error
		result interface{}
	)
	for i := 0; i < tryTimes; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), rs.timeout)
		result, err = rs.nodeHost.SyncRead(ctx, shardID, query)
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

func (rs *RedisServer) cleanResources() {
	rs.conns.Range(func(key, val any) bool {
		name, ok := key.(string)
		if ok {
			log.Println("Clean connection", name)
			conn, ok := val.(*rclient)
			if ok {
				conn.Close()
			}
		}
		return true
	})
}

func (rs *RedisServer) Close() error {
	rs.stop = true
	rs.cleanResources()
	if rs.ln != nil {
		return rs.ln.Close()
	}
	return nil
}
