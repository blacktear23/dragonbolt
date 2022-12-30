package server

import (
	"github.com/blacktear23/dragonbolt/protocol"
	"github.com/blacktear23/dragonbolt/query"
)

func (c *rclient) handleQuery(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 1 {
		return protocol.NewSimpleError("Need more arguments")
	}
	query, err := c.parseString(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}

	autoCommit, err := c.autoBegin()
	if err != nil {
		return protocol.NewSimpleErrorf("Transaction Error: %v", err)
	}

	ret, err := c.processQuery(query)
	if err != nil {
		if autoCommit {
			c.autoRollback()
		}
		return protocol.NewSimpleErrorf("Internal error: %v", err)
	}
	if autoCommit {
		err = c.autoCommit()
		if err != nil {
			return protocol.NewSimpleErrorf("Transaction Error: %v", err)
		}
	}
	return ret
}

func (c *rclient) processQuery(queryStr string) (protocol.Encodable, error) {
	exec, err := query.BuildExecutor(queryStr)
	if err != nil {
		return nil, err
	}
	iter, err := c.txn.Cursor()
	if err != nil {
		return nil, err
	}
	err = iter.Seek([]byte{})
	if err != nil {
		return nil, err
	}
	ret := protocol.Array{}
	for {
		key, val, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}
		ok, err := exec.Filter(query.NewKVP(key, val))
		if err != nil {
			return nil, err
		}
		if ok {
			ret = append(ret, protocol.Array{protocol.NewBlobString(key), protocol.NewBlobString(val)})
		}
	}
	return ret, nil
}
