package query

import (
	"bytes"
	"fmt"
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
	RANGE  byte = 4
	FULL   byte = 5
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
	case RANGE:
		if len(stype.keys) == 2 {
			return NewRangeScanPlan(o.txn, o.filter, stype.keys[0], stype.keys[1])
		}
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
		case Gt, Gte:
			// It may use RANGE or FULL
			return o.optimizeGtGteExpr(e)
		case Lt, Lte:
			// It may use RANGE or FULL
			return o.optimizeLtLteExpr(e)
		default:
			// Other operator use FULL
			return &ScanType{FULL, nil}
		}
	default:
		// Other expression use FULL
		return &ScanType{FULL, nil}
	}
}

func (o *FilterOptimizer) optimizeGtGteExpr(e *BinaryOpExpr) *ScanType {
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

	// Is Key start vale and value can calculate in query,
	// return RANGE scan with start
	if field == KeyKW && key != nil {
		return &ScanType{RANGE, [][]byte{key, nil}}
	}

	// If not just return FULL scan
	return &ScanType{FULL, nil}
}

func (o *FilterOptimizer) optimizeLtLteExpr(e *BinaryOpExpr) *ScanType {
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

	// Is Key start vale and value can calculate in query,
	// return RANGE scan with end
	if field == KeyKW && key != nil {
		return &ScanType{RANGE, [][]byte{nil, key}}
	}

	// If not just return FULL scan
	return &ScanType{FULL, nil}
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
		case RANGE:
			// Intersection two range
			return o.intersectionRange(lstype, rstype)
		}
		return lstype
	}

	// just return lower priority of scan type operation
	if lstype.scanTp < rstype.scanTp {
		if lstype.scanTp == MGET && rstype.scanTp == PREFIX {
			// Process MGET & PREFIX, it may use MGET or EMPTY
			return o.intersectionMgetAndPrefix(lstype, rstype)
		}
		if rstype.scanTp == RANGE {
			switch lstype.scanTp {
			case MGET:
				// Process MGET & RANGE, it may use MGET or EMPTY
				return o.intersectionMgetAndRange(lstype, rstype)
			case PREFIX:
				// Process PREFIX & RANGE, it may use PREFIX, RANGE, EMPTY or FULL
				return o.intersectionPrefixAndRange(lstype, rstype)
			}
		}
		return lstype
	}
	if rstype.scanTp == MGET && lstype.scanTp == PREFIX {
		// Process MGET & PREFIX, it may use MGET or EMPTY
		return o.intersectionMgetAndPrefix(rstype, lstype)
	}
	if lstype.scanTp == RANGE {
		switch rstype.scanTp {
		case MGET:
			// Process MGET & RANGE, it may use MGET or EMPTY
			return o.intersectionMgetAndRange(rstype, lstype)
		case PREFIX:
			// Process PREFIX & RANGE, it may use PREFIX, RANGE, EMPTY or FULL
			return o.intersectionPrefixAndRange(rstype, lstype)
		}
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
		case RANGE:
			// Union two range
			return o.unionRange(lstype, rstype)
		}
		return lstype
	}

	// just return higher priority scan type operation
	if lstype.scanTp < rstype.scanTp {
		if lstype.scanTp == MGET && rstype.scanTp == PREFIX {
			// Process MGET | PREFIX, it may use PREFIX or FULL
			return o.unionMgetAndPrefix(lstype, rstype)
		}
		if rstype.scanTp == RANGE {
			switch lstype.scanTp {
			case MGET:
				// Process MGET | RANGE, it may use RANGE or FULL
				return o.unionMgetAndRange(lstype, rstype)
			case PREFIX:
				// Process PREFIX | RANGE, it may use PREFIX, RANGE or FULL
				return o.unionPrefixAndRange(lstype, rstype)
			}
		}
		return rstype
	}
	if rstype.scanTp == MGET && rstype.scanTp == PREFIX {
		// Process MGET | PREFIX, it may use PREFIX or FULL
		return o.unionMgetAndPrefix(rstype, lstype)
	}
	if lstype.scanTp == RANGE {
		switch rstype.scanTp {
		case MGET:
			// Process MGET | RANGE, it may use RANGE or FULL
			return o.unionMgetAndRange(rstype, lstype)
		case PREFIX:
			// Process PREFIX | RANGE, it may use PREFIX, RANGE or FULL
			return o.unionPrefixAndRange(rstype, lstype)
		}
	}
	return lstype
}

func (o *FilterOptimizer) intersectionMgetAndPrefix(mget, prefix *ScanType) *ScanType {
	if len(prefix.keys) == 0 {
		fmt.Println("[ALRET] error at intersectionMgetAndPrefix invalid prefix")
		return &ScanType{FULL, nil}
	}
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
	if len(prefix.keys) == 0 {
		fmt.Println("[ALRET] error at unionMgetAndPrefix invalid prefix")
		return &ScanType{FULL, nil}
	}
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

func inRange(start, end, val []byte, isEnd bool) bool {
	if start == nil && end != nil {
		if val == nil && !isEnd {
			return true
		} else if val == nil && isEnd {
			return false
		}
		return bytes.Compare(end, val) >= 0
	}
	if start != nil && end == nil {
		if val == nil && !isEnd {
			return false
		} else if val == nil && isEnd {
			return true
		}
		return bytes.Compare(start, val) <= 0
	}
	return bytes.Compare(start, val) <= 0 && bytes.Compare(end, val) >= 0
}

func (o *FilterOptimizer) intersectionRange(l, r *ScanType) *ScanType {
	if len(l.keys) != 2 || len(r.keys) != 2 {
		return &ScanType{FULL, nil}
	}
	lstart, lend := l.keys[0], l.keys[1]
	rstart, rend := r.keys[0], r.keys[1]

	if lstart != nil && lend != nil && bytes.Compare(lstart, lend) > 0 {
		lstart, lend = lend, lstart
	}
	if rstart != nil && rend != nil && bytes.Compare(rstart, rend) > 0 {
		rstart, rend = rend, rstart
	}

	// Same range just return left
	if bytes.Compare(lstart, rstart) == 0 && bytes.Compare(lend, rend) == 0 {
		return l
	}

	var (
		nstart []byte = nil
		nend   []byte = nil
	)

	// | ^LS,RS | LE,RE$ |
	// just use full scan instead
	if lstart == nil && rstart == nil && lend == nil && rend == nil {
		return &ScanType{FULL, nil}
	}

	if inRange(lstart, lend, rstart, false) && !inRange(lstart, lend, rend, true) {
		// | LS | RS | LE | RE |
		nstart = rstart
		nend = lend
	} else if inRange(rstart, rend, lstart, false) && !inRange(rstart, rend, lend, true) {
		// | RS | LS | RE | LE |
		nstart = lstart
		nend = rend
	} else if inRange(lstart, lend, rstart, false) && inRange(lstart, lend, rend, true) {
		// | LS | RS | RE | LE |
		nstart = rstart
		nend = rend
	} else if inRange(rstart, rend, lstart, false) && inRange(rstart, rend, lend, true) {
		// | RS | LS | LE | RE |
		nstart = lstart
		nend = lend
	} else if !inRange(lstart, lend, rstart, false) && !inRange(lstart, lend, rend, true) {
		// | LS | LE | RS | RE |
		// | RS | RE | LS | LE |
		// No result just return EMPTY
		return &ScanType{EMPTY, nil}
	}

	if nstart == nil && nend == nil {
		return &ScanType{FULL, nil}
	}

	// start == end just use MGET
	if bytes.Compare(nstart, nend) == 0 {
		return &ScanType{MGET, [][]byte{nstart}}
	}

	return &ScanType{RANGE, [][]byte{nstart, nend}}
}

func (o *FilterOptimizer) unionRange(l, r *ScanType) *ScanType {
	if len(l.keys) != 2 || len(r.keys) != 2 {
		return &ScanType{FULL, nil}
	}
	lstart, lend := l.keys[0], l.keys[1]
	rstart, rend := r.keys[0], r.keys[1]

	if lstart != nil && lend != nil && bytes.Compare(lstart, lend) > 0 {
		lstart, lend = lend, lstart
	}
	if rstart != nil && rend != nil && bytes.Compare(rstart, rend) > 0 {
		rstart, rend = rend, rstart
	}

	// Same range just return left
	if bytes.Compare(lstart, rstart) == 0 && bytes.Compare(lend, rend) == 0 {
		return l
	}

	var (
		nstart []byte = nil
		nend   []byte = nil
	)

	// | ^LS,RS | LE,RE$ |
	// just use full scan instead
	if lstart == nil && rstart == nil && lend == nil && rend == nil {
		return &ScanType{FULL, nil}
	}

	if inRange(lstart, lend, rstart, false) && !inRange(lstart, lend, rend, true) {
		// | LS | RS | LE | RE |
		nstart = lstart
		nend = rend
	} else if inRange(rstart, rend, lstart, false) && !inRange(rstart, rend, lend, true) {
		// | RS | LS | RE | LE |
		nstart = rstart
		nend = lend
	} else if inRange(lstart, lend, rstart, false) && inRange(lstart, lend, rend, true) {
		// | LS | RS | RE | LE |
		nstart = lstart
		nend = lend
	} else if inRange(rstart, rend, lstart, false) && inRange(rstart, rend, lend, true) {
		// | RS | LS | LE | RE |
		nstart = rstart
		nend = rend
	} else if !inRange(lstart, lend, rstart, false) && !inRange(lstart, lend, rend, true) {
		if inRange(lstart, rstart, lend, true) {
			// | LS | LE | RS | RE |
			nstart = lstart
			nend = rend
		} else if inRange(rstart, lstart, rend, true) {
			// | RS | RE | LS | LE |
			nstart = rstart
			nend = lend
		}
	}

	if nstart == nil && nend == nil {
		return &ScanType{FULL, nil}
	}

	// start == end just use MGET scan
	if bytes.Compare(nstart, nend) == 0 {
		return &ScanType{MGET, [][]byte{nstart}}
	}
	return &ScanType{RANGE, [][]byte{nstart, nend}}
}

func (o *FilterOptimizer) intersectionMgetAndRange(mget, srange *ScanType) *ScanType {
	if len(srange.keys) != 2 {
		fmt.Println("[ALRET] error at intersectionMgetAndRange invalid range")
		return &ScanType{FULL, nil}
	}
	ikeys := [][]byte{}
	rstart, rend := srange.keys[0], srange.keys[1]

	// Check keys is in range
	for _, k := range mget.keys {
		if inRange(rstart, rend, k, false) {
			ikeys = append(ikeys, k)
		}
	}

	// If no keys in range, just return empty scan
	if len(ikeys) == 0 {
		return &ScanType{EMPTY, nil}
	}

	// Return in range keys with mget scan
	return &ScanType{MGET, ikeys}
}

func (o *FilterOptimizer) intersectionPrefixAndRange(prefix, srange *ScanType) *ScanType {
	if len(srange.keys) != 2 {
		fmt.Println("[ALRET] error at intersectionPrefixAndRange invalid range")
		return &ScanType{FULL, nil}
	}
	if len(prefix.keys) == 0 {
		fmt.Println("[ALRET] error at intersectionPrefixAndRange invalid prefix")
		return &ScanType{FULL, nil}
	}
	pstart := prefix.keys[0]
	pend := append(pstart, 0xFF)
	rstart, rend := srange.keys[0], srange.keys[1]

	if inRange(rstart, rend, pstart, false) && !inRange(rstart, rend, pend, true) {
		// | RS | PS | RE | PE |
		// make sure the result is correct just use PREFIX scan
		return prefix
	} else if inRange(pstart, pend, rstart, false) && !inRange(pstart, pend, rend, true) {
		// | PS | RS | PE | RE |
		// make sure the result is correct just use PREFIX scan
		return prefix
	} else if inRange(rstart, rend, pstart, false) && inRange(rstart, rend, pend, true) {
		// | RS | PS | PE | RE |
		// Just use PREFIX scan
		return prefix
	} else if inRange(pstart, pend, rstart, false) && inRange(pstart, pend, rend, true) {
		// | PS | RS | RE | PE |
		// Just use RANGE scan
		return srange
	} else if !inRange(rstart, rend, pstart, false) && !inRange(rstart, rend, pend, true) {
		// | PS | PE | RS | RE |
		// | RS | RE | PS | PE |
		// Just use EMPTY scan
		return &ScanType{EMPTY, nil}
	}
	// If we get there cannot find out which scan type is better just use FULL
	// scan to make sure the correctness
	return &ScanType{FULL, nil}
}

func (o *FilterOptimizer) unionMgetAndRange(mget, srange *ScanType) *ScanType {
	if len(srange.keys) != 2 {
		fmt.Println("[ALRET] error at intersectionPrefixAndRange invalid range")
		return &ScanType{FULL, nil}
	}
	haveRangeNotMatch := false
	rstart, rend := srange.keys[0], srange.keys[1]

	// Check keys is in range
	for _, k := range mget.keys {
		if !inRange(rstart, rend, k, false) {
			haveRangeNotMatch = true
			break
		}
	}

	// If there has one mget key not in the range just use full scan
	if haveRangeNotMatch {
		return &ScanType{FULL, nil}
	}

	// All keys are in range just return the range scan
	return srange
}

func (o *FilterOptimizer) unionPrefixAndRange(prefix, srange *ScanType) *ScanType {
	if len(srange.keys) != 2 {
		fmt.Println("[ALRET] error at intersectionPrefixAndRange invalid range")
		return &ScanType{FULL, nil}
	}
	if len(prefix.keys) == 0 {
		fmt.Println("[ALRET] error at intersectionPrefixAndRange invalid prefix")
		return &ScanType{FULL, nil}
	}

	pstart := prefix.keys[0]
	pend := append(pstart, 0xFF)
	rstart, rend := srange.keys[0], srange.keys[1]

	if inRange(pstart, pend, rstart, false) && inRange(pstart, pend, rend, true) {
		// | PS | RS | RE | PE |
		// Just use PREFIX scan
		return prefix
	} else if inRange(rstart, rend, pstart, false) && inRange(rstart, rend, pend, true) {
		// | RS | PS | PE | RE |
		// Just use RANGE scan
		return srange
	}
	// If we get there we cannot find out which scan type is better just use FULL
	// scan to make sure the correctness
	return &ScanType{FULL, nil}
}
