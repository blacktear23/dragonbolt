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
		cols, err := plan.Next()
		if err != nil {
			return nil, err
		}
		if cols == nil {
			break
		}
		fields := make([]protocol.Encodable, len(cols))
		for i := 0; i < len(cols); i++ {
			fields[i] = protocol.NewBlobString(cols[i])
		}
		ret = append(ret, protocol.Array(fields))
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

	ret := protocol.Array{}
	for i, plan := range plan.Explain() {
		space := ""
		for x := 0; x < i*3; x++ {
			space += " "
		}
		var planStr string
		if i == 0 {
			planStr = space + plan
		} else {
			planStr = space + "`-" + plan
		}
		ret = append(ret, protocol.NewSimpleString(planStr))
	}

	return ret, nil
}
