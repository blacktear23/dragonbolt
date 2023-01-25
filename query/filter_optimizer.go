package query

import (
	"bytes"
	"strings"

	"github.com/blacktear23/dragonbolt/txn"
)

/*
 * Scan Type is priority of scan operator
 * Lower value operator means the result set is smaller than higher value operator
 */
const (
	EMPTY  byte = 1
	MGET   byte = 2
	PREFIX byte = 3
	FULL   byte = 4
)

type ScanType struct {
	scanTp byte
	keys   [][]byte
}

type FilterOptimizer struct {
	expr   Expression
	filter *FilterExec
	txn    txn.Txn
}

func NewFilterOptimizer(ast *WhereStmt, t txn.Txn, filter *FilterExec) *FilterOptimizer {
	return &FilterOptimizer{
		expr:   ast.Expr,
		txn:    t,
		filter: filter,
	}
}

func (o *FilterOptimizer) Optimize() Plan {
	stype := o.optimizeExpr(o.expr)
	switch stype.scanTp {
	case EMPTY:
		return NewEmptyResultPlan(o.txn, o.filter)
	case MGET:
		skeys := make([]string, len(stype.keys))
		for i, k := range stype.keys {
			skeys[i] = string(k)
		}
		return NewMultiGetPlan(o.txn, o.filter, skeys)
	case PREFIX:
		if len(stype.keys) == 0 {
			return NewFullScanPlan(o.txn, o.filter)
		}
		prefix := string(stype.keys[0])
		return NewPrefixScanPlan(o.txn, o.filter, prefix)
	case FULL:
		return NewFullScanPlan(o.txn, o.filter)
	}
	// No match just return full scan plan
	return NewFullScanPlan(o.txn, o.filter)
}

func (o *FilterOptimizer) optimizeExpr(expr Expression) *ScanType {
	switch e := expr.(type) {
	case *BinaryOpExpr:
		switch e.Op {
		case And:
			return o.optimizeAndExpr(e)
		case Or:
			return o.optimizeOrExpr(e)
		case PrefixMatch:
			// It may use PREFIX or FULL
			return o.optimizePrefixMatchExpr(e)
		case Eq:
			// It may use MGET or FULL
			return o.optimizeEqualExpr(e)
		default:
			// Other operator use FULL
			return &ScanType{FULL, nil}
		}
	default:
		// Other expression use FULL
		return &ScanType{FULL, nil}
	}
}

func (o *FilterOptimizer) optimizePrefixMatchExpr(e *BinaryOpExpr) *ScanType {
	var (
		field KVKeyword = ValueKW
		key   []byte    = nil
	)

	switch left := e.Left.(type) {
	case *StringExpr:
		key = []byte(left.Data)
	case *FieldExpr:
		field = left.Field
	}

	switch right := e.Right.(type) {
	case *StringExpr:
		key = []byte(right.Data)
	case *FieldExpr:
		field = right.Field
	}

	// Is Key prefix scan value and value can calculate in query,
	// return PREFIX scan
	if field == KeyKW && key != nil {
		return &ScanType{PREFIX, [][]byte{key}}
	}
	// If not just return FULL scan
	return &ScanType{FULL, nil}
}

func (o *FilterOptimizer) optimizeEqualExpr(e *BinaryOpExpr) *ScanType {
	var (
		field KVKeyword = ValueKW
		key   []byte    = nil
	)

	switch left := e.Left.(type) {
	case *StringExpr:
		key = []byte(left.Data)
	case *FieldExpr:
		field = left.Field
	}

	switch right := e.Right.(type) {
	case *StringExpr:
		key = []byte(right.Data)
	case *FieldExpr:
		field = right.Field
	}

	// Is Key equals value and value can calculate in query
	// return MGET scan
	if field == KeyKW && key != nil {
		return &ScanType{MGET, [][]byte{key}}
	}

	// If not just return FULL scan
	return &ScanType{FULL, nil}
}

func (o *FilterOptimizer) optimizeAndExpr(e *BinaryOpExpr) *ScanType {
	lstype := o.optimizeExpr(e.Left)
	rstype := o.optimizeExpr(e.Right)
	if lstype.scanTp == rstype.scanTp {
		switch lstype.scanTp {
		case MGET:
			// Intersection two mget scan operation keys
			return o.intersectionMget(lstype, rstype)
		case PREFIX:
			// Intersection two prefix scan operation prefixes
			return o.intersectionPrefix(lstype, rstype)
		}
		return lstype
	}

	// just return lower priority of scan type operation
	if lstype.scanTp < rstype.scanTp {
		if lstype.scanTp == MGET && rstype.scanTp == PREFIX {
			// Process MGET & PREFIX, it may use MGET or EMPTY
			return o.intersectionMgetAndPrefix(lstype, rstype)
		}
		return lstype
	}
	if rstype.scanTp == MGET && lstype.scanTp == PREFIX {
		// Process MGET & PREFIX, it may use MGET or EMPTY
		return o.intersectionMgetAndPrefix(rstype, lstype)
	}
	return rstype
}

func (o *FilterOptimizer) optimizeOrExpr(e *BinaryOpExpr) *ScanType {
	lstype := o.optimizeExpr(e.Left)
	rstype := o.optimizeExpr(e.Right)
	if lstype.scanTp == rstype.scanTp {
		switch lstype.scanTp {
		case MGET:
			// Union two mget scan operation keys
			return o.unionMget(lstype, rstype)
		case PREFIX:
			// Union two prefix scan operation prefixes
			return o.unionPrefix(lstype, rstype)
		}
		return lstype
	}

	// just return higher priority scan type operation
	if lstype.scanTp < rstype.scanTp {
		if lstype.scanTp == MGET && rstype.scanTp == PREFIX {
			// Process MGET | PREFIX, it may use PREFIX or FULL
			return o.unionMgetAndPrefix(lstype, rstype)
		}
		return rstype
	}
	if rstype.scanTp == MGET && rstype.scanTp == PREFIX {
		// Process MGET | PREFIX, it may use PREFIX or FULL
		return o.unionMgetAndPrefix(rstype, lstype)
	}
	return lstype
}

func (o *FilterOptimizer) intersectionMgetAndPrefix(mget, prefix *ScanType) *ScanType {
	ikeys := [][]byte{}
	prefixKey := prefix.keys[0]

	// Check keys with prefix
	for _, k := range mget.keys {
		if bytes.HasPrefix(k, prefixKey) {
			ikeys = append(ikeys, k)
		}
	}
	// If no keys match prefix, just return empty scan
	if len(ikeys) == 0 {
		return &ScanType{EMPTY, nil}
	}

	// Return matched prefix keys with mget scan
	return &ScanType{MGET, ikeys}
}

func (o *FilterOptimizer) unionMgetAndPrefix(mget, prefix *ScanType) *ScanType {
	havePrefixNotMatch := false
	prefixKey := prefix.keys[0]

	// Check keys with prefix
	for _, k := range mget.keys {
		if !bytes.HasPrefix(k, prefixKey) {
			havePrefixNotMatch = true
			break
		}
	}

	// If there has one mget key not has the prefix just use full scan
	if havePrefixNotMatch {
		return &ScanType{FULL, nil}
	}
	// All keys match prefix just return prefix scan
	return prefix
}

func (o *FilterOptimizer) intersectionMget(l, r *ScanType) *ScanType {
	keys := [][]byte{}
	lkeys := map[string][]byte{}
	rkeys := map[string][]byte{}
	for _, k := range l.keys {
		lkeys[string(k)] = k
	}

	for _, k := range r.keys {
		rkeys[string(k)] = k
	}

	// filter the keys that in both keys
	for lk, lv := range lkeys {
		if _, have := rkeys[lk]; have {
			keys = append(keys, lv)
		}
	}

	if len(keys) == 0 {
		// No keys just return empty scan
		return &ScanType{EMPTY, nil}
	}
	return &ScanType{MGET, keys}
}

func (o *FilterOptimizer) unionMget(l, r *ScanType) *ScanType {
	keys := [][]byte{}
	ukeys := map[string][]byte{}

	// Merge two mget scan keys into one map
	for _, k := range l.keys {
		ukeys[string(k)] = k
	}

	for _, k := range r.keys {
		ukeys[string(k)] = k
	}

	for _, v := range ukeys {
		keys = append(keys, v)
	}
	if len(keys) == 0 {
		// No keys just return empty scan
		return &ScanType{EMPTY, nil}
	}
	return &ScanType{MGET, keys}
}

func (o *FilterOptimizer) intersectionPrefix(l, r *ScanType) *ScanType {
	lks := string(l.keys[0])
	rks := string(r.keys[0])

	// If two prefix is same just return left
	if lks == rks {
		return l
	}

	// Find the longest prefix
	if lks < rks && strings.HasPrefix(rks, lks) {
		return r
	}

	if rks < lks && strings.HasPrefix(lks, rks) {
		return l
	}

	// short prefix is not long prefix's prefix means no keys should be scan
	// just return empty scan
	return &ScanType{EMPTY, nil}
}

func (o *FilterOptimizer) unionPrefix(l, r *ScanType) *ScanType {
	lks := string(l.keys[0])
	rks := string(r.keys[0])

	// If two prefix is same just return left
	if lks == rks {
		return l
	}

	// Find the shortest prefix
	if lks < rks && strings.HasPrefix(rks, lks) {
		return l
	}

	if rks < lks && strings.HasPrefix(lks, rks) {
		return r
	}

	// short prefix is not long prefix's prefix means all keys should be scan
	// just return full scan
	return &ScanType{FULL, nil}
}
