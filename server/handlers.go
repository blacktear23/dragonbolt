package server

import (
	"bytes"

	"github.com/blacktear23/dragonbolt/protocol"
)

func (c *rclient) handleCommand(cmd string, args []protocol.Encodable) protocol.Encodable {
	switch cmd {
	case "command":
		return c.handleCmd(args)
	case "ping":
		return protocol.NewSimpleString("PONG")
	case "config":
		return protocol.NewSimpleString("OK")
	case "get":
		return c.handleGet(args)
	case "set":
		return c.handleSet(args)
	case "del":
		return c.handleDel(args)
	case "mget":
		return c.handleMget(args)
	case "mset":
		return c.handleMset(args)
	case "inc", "incr":
		return c.handleIncDec(args, 1)
	case "dec", "decr":
		return c.handleIncDec(args, -1)
	case "scan":
		return c.handleScan(args)
	case "cf.set":
		return c.handleCfSet(args)
	case "cf.get":
		return c.handleCfGet(args)
	case "cf.del":
		return c.handleCfDel(args)
	case "cf.scan":
		return c.handleCfScan(args, false)
	case "cf.rscan":
		return c.handleCfScan(args, true)
	case "begin", "txn.begin":
		return c.handleBegin(args)
	case "tset", "txn.set":
		return c.handleTxnSet(args)
	case "tget", "txn.get":
		return c.handleTxnGet(args)
	case "tmset", "txn.mset":
		return c.handleTxnMset(args)
	case "tmget", "txn.mget":
		return c.handleTxnMget(args)
	case "tdel", "txn.del":
		return c.handleTxnDelete(args)
	case "tscan", "txn.scan":
		return c.handleTxnScan(args)
	case "tlock", "txn.lock":
		return c.handleTxnLock(args)
	case "tunlock", "txn.unlock":
		return c.handleTxnUnlock(args)
	case "commit", "txn.commit":
		return c.handleCommit(args)
	case "rollback", "txn.rollback":
		return c.handleRollback(args)
	case "db.create":
		return c.handleCreateDB(args)
	case "db.use":
		return c.handleUseDB(args)
	case "db.list":
		return c.handleListDB(args)
	case "db.curr", "db.current":
		return c.handleCurrentDB(args)
	case "db.del", "db.delete":
		return c.handleDeleteDB(args)
	default:
		return protocol.NewSimpleErrorf("Unsupport command: %s", cmd)
	}
}

func (c *rclient) handleCmd(args []protocol.Encodable) protocol.Encodable {
	return protocol.NewSimpleString("OK")
}

func (c *rclient) handleTSO(args []protocol.Encodable) protocol.Encodable {
	tso, err := c.rs.tsoSrv.GetTSO()
	if err != nil {
		return protocol.NewSimpleErrorf("Internal Error: %v", err)
	}
	return protocol.NewNumberUint(tso)
}

func keyCompare(val1 []byte, val2 []byte) int {
	if val2 == nil {
		return -1
	}
	return bytes.Compare(val1, val2)
}
