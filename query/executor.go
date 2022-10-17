package query

import (
	"bytes"
	"errors"
	"regexp"
)

var (
	ErrNotEnoughOperators       = errors.New("Not enough join operators")
	ErrUnknownLeftKeyword       = errors.New("Unknown left keyword")
	ErrUnsupportCompareOperator = errors.New("Unsupport compare operator")
	ErrNoCompareExists          = errors.New("No compare expression exists")
)

type KVPair struct {
	Key   []byte
	Value []byte
}

func NewKVP(key []byte, val []byte) KVPair {
	return KVPair{
		Key:   key,
		Value: val,
	}
}

func NewKVPStr(key string, val string) KVPair {
	return KVPair{
		Key:   []byte(key),
		Value: []byte(val),
	}
}

type FilterExec struct {
	Ast *WhereStmt
}

func (e *FilterExec) Filter(kvp KVPair) (bool, error) {
	ret, err := e.FilterBatch([]KVPair{kvp})
	if err != nil {
		return false, err
	}
	return ret[0], nil
}

func (e *FilterExec) FilterBatch(kvps []KVPair) ([]bool, error) {
	ret := make([]bool, len(kvps))
	for idx, kvp := range kvps {
		result, err := e.Ast.Expr.Execute(kvp)
		if err != nil {
			return nil, err
		}
		bresult, ok := result.(bool)
		if !ok {
			return nil, errors.New("Expression result is not boolean")
		}
		ret[idx] = bresult
	}
	return ret, nil
}

func (e *StringExpr) Execute(kv KVPair) (any, error) {
	return []byte(e.Data), nil
}

func (e *FieldExpr) Execute(kv KVPair) (any, error) {
	switch e.Field {
	case KeyKW:
		return kv.Key, nil
	case ValueKW:
		return kv.Value, nil
	}
	return nil, errors.New("Invalid Field")
}

func (e *CompareExpr) Execute(kv KVPair) (any, error) {
	switch e.Op {
	case Eq:
		return e.execEqual(kv)
	case NotEq:
		return e.execNotEqual(kv)
	case PrefixMatch:
		return e.execPrefixMatch(kv)
	case RegExpMatch:
		return e.execRegexpMatch(kv)
	case And:
		return e.execAnd(kv)
	case Or:
		return e.execOr(kv)
	}
	return nil, errors.New("Unknown operator")
}

func (e *CompareExpr) execEqual(kv KVPair) (bool, error) {
	rleft, err := e.Left.Execute(kv)
	if err != nil {
		return false, err
	}
	rright, err := e.Right.Execute(kv)
	if err != nil {
		return false, err
	}
	left, lok := rleft.([]byte)
	right, rok := rright.([]byte)
	if !lok || !rok {
		return false, errors.New("= left value error")
	}
	return bytes.Equal(left, right), nil
}

func (e *CompareExpr) execNotEqual(kv KVPair) (bool, error) {
	ret, err := e.execEqual(kv)
	if err != nil {
		return false, err
	}
	return !ret, nil
}

func (e *CompareExpr) execPrefixMatch(kv KVPair) (bool, error) {
	rleft, err := e.Left.Execute(kv)
	if err != nil {
		return false, err
	}
	rright, err := e.Right.Execute(kv)
	if err != nil {
		return false, err
	}
	left, lok := rleft.([]byte)
	right, rok := rright.([]byte)
	if !lok || !rok {
		return false, errors.New("^= left value error")
	}
	return bytes.HasPrefix(left, right), nil
}

func (e *CompareExpr) execRegexpMatch(kv KVPair) (bool, error) {
	rleft, err := e.Left.Execute(kv)
	if err != nil {
		return false, err
	}
	rright, err := e.Right.Execute(kv)
	if err != nil {
		return false, err
	}
	left, lok := rleft.([]byte)
	right, rok := rright.([]byte)
	if !lok || !rok {
		return false, errors.New("~= left value error")
	}
	re, err := regexp.Compile(string(right))
	if err != nil {
		return false, err
	}
	return re.Match(left), nil
}

func (e *CompareExpr) execAnd(kv KVPair) (bool, error) {
	rleft, err := e.Left.Execute(kv)
	if err != nil {
		return false, err
	}
	rright, err := e.Right.Execute(kv)
	if err != nil {
		return false, err
	}
	left, lok := rleft.(bool)
	right, rok := rright.(bool)
	if !lok || !rok {
		return false, errors.New("& left value error")
	}
	return left && right, nil
}

func (e *CompareExpr) execOr(kv KVPair) (bool, error) {
	rleft, err := e.Left.Execute(kv)
	if err != nil {
		return false, err
	}
	rright, err := e.Right.Execute(kv)
	if err != nil {
		return false, err
	}
	left, lok := rleft.(bool)
	right, rok := rright.(bool)
	if !lok || !rok {
		return false, errors.New("| left value error")
	}
	return left || right, nil
}

func (e *NotExpr) Execute(kv KVPair) (any, error) {
	rright, err := e.Right.Execute(kv)
	if err != nil {
		return nil, err
	}
	right, rok := rright.(bool)
	if !rok {
		return nil, errors.New("! right value error")
	}
	return !right, nil
}
