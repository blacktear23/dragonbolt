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
	optimizer := query.NewOptimizer(queryStr)
	plan, err := optimizer.BuildPlan(c.txn)
	if err != nil {
		return nil, err
	}
	ret := protocol.Array{}
	for {
		key, val, err := plan.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}
		ret = append(ret, protocol.Array{protocol.NewBlobString(key), protocol.NewBlobString(val)})
	}
	return ret, nil
}

func (c *rclient) handleExplain(args []protocol.Encodable) protocol.Encodable {
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

	ret, err := c.processExplain(query)
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

func (c *rclient) processExplain(queryStr string) (protocol.Encodable, error) {
	optimizer := query.NewOptimizer(queryStr)
	plan, err := optimizer.BuildPlan(c.txn)
	if err != nil {
		return nil, err
	}
	ret := plan.String()
	return protocol.NewBlobString([]byte(ret)), nil
}
