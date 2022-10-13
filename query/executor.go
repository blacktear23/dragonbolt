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
	if len(e.Ast.Expr.Compares) == 0 {
		return nil, ErrNoCompareExists
	}
	ret := make([]bool, len(kvps))
	for idx, kvp := range kvps {
		compRets := make([]bool, len(e.Ast.Expr.Compares))
		for i, kvc := range e.Ast.Expr.Compares {
			cmpRet, err := e.executeCompare(kvc, kvp)
			if err != nil {
				return nil, err
			}
			compRets[i] = cmpRet
		}
		if len(compRets) > 1 {
			left := compRets[0]
			for i, right := range compRets[1:] {
				if i >= len(e.Ast.Expr.JoinOperators) {
					return nil, ErrNotEnoughOperators
				}
				op := e.Ast.Expr.JoinOperators[i]
				switch op {
				case And:
					left = left && right
				case Or:
					left = left || right
				}
			}
			ret[idx] = left
		} else {
			ret[idx] = compRets[0]
		}
	}
	return ret, nil
}

func (e *FilterExec) executeCompare(kvc KVCompareExpr, kvp KVPair) (bool, error) {
	switch kvc.Op {
	case Eq:
		return e.execEqualCmp(kvc, kvp)
	case PrefixMatch:
		return e.execPrefixMatch(kvc, kvp)
	case RegExpMatch:
		return e.execRegexpMatch(kvc, kvp)
	}
	return false, ErrUnsupportCompareOperator
}

func (e *FilterExec) execEqualCmp(kvc KVCompareExpr, kvp KVPair) (bool, error) {
	switch kvc.Left {
	case KeyKW:
		return bytes.Equal(kvp.Key, []byte(kvc.Right)), nil
	case ValueKW:
		return bytes.Equal(kvp.Value, []byte(kvc.Right)), nil
	}
	return false, ErrUnknownLeftKeyword
}

func (e *FilterExec) execPrefixMatch(kvc KVCompareExpr, kvp KVPair) (bool, error) {
	switch kvc.Left {
	case KeyKW:
		return bytes.HasPrefix(kvp.Key, []byte(kvc.Right)), nil
	case ValueKW:
		return bytes.HasPrefix(kvp.Value, []byte(kvc.Right)), nil
	}
	return false, ErrUnknownLeftKeyword
}

func (e *FilterExec) execRegexpMatch(kvc KVCompareExpr, kvp KVPair) (bool, error) {
	re, err := regexp.Compile(kvc.Right)
	if err != nil {
		return false, err
	}
	switch kvc.Left {
	case KeyKW:
		return re.Match(kvp.Key), nil
	case ValueKW:
		return re.Match(kvp.Value), nil
	}
	return false, ErrUnknownLeftKeyword
}
