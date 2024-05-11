package server

import (
	"fmt"
	"strings"

	"github.com/blacktear23/dragonbolt/protocol"
	"github.com/blacktear23/dragonbolt/query"
	"github.com/c4pt0r/kvql"
)

func multiLineErrorf(format string, args ...any) protocol.Array {
	msg := fmt.Sprintf(format, args...)
	ret := make([]protocol.Encodable, 0, 3)
	for _, line := range strings.Split(msg, "\n") {
		ret = append(ret, protocol.NewSimpleError(line))
	}
	return ret
}

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
		if qerr, ok := err.(kvql.QueryBinder); ok {
			qerr.BindQuery(query)
			qerr.SetPadding(0)
			return multiLineErrorf(err.Error())
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

func convertToBytes(val any) []byte {
	switch v := val.(type) {
	case string:
		return []byte(v)
	case []byte:
		return v
	case int, int8, int16, int32, int64,
		uint, uint16, uint32, uint64:
		return []byte(fmt.Sprintf("%v", v))
	case byte:
		return []byte{v}
	case bool:
		if v {
			return []byte("true")
		} else {
			return []byte("false")
		}
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}

func (c *rclient) processQuery(queryStr string) (protocol.Encodable, error) {
	optimizer := kvql.NewOptimizer(queryStr)
	store := query.NewTxnStore(c.txn)
	plan, err := optimizer.BuildPlan(store)
	if err != nil {
		return nil, err
	}
	ret := protocol.Array{}
	execCtx := kvql.NewExecuteCtx()
	for {
		cols, err := plan.Next(execCtx)
		if err != nil {
			return nil, err
		}
		if cols == nil {
			break
		}
		execCtx.Clear()
		fields := make([]protocol.Encodable, len(cols))
		for i := 0; i < len(cols); i++ {
			fields[i] = protocol.NewBlobString(convertToBytes(cols[i]))
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
		if qerr, ok := err.(kvql.QueryBinder); ok {
			qerr.BindQuery(query)
			qerr.SetPadding(0)
			return multiLineErrorf(err.Error())
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
	optimizer := kvql.NewOptimizer(queryStr)
	store := query.NewTxnStore(c.txn)
	plan, err := optimizer.BuildPlan(store)
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
