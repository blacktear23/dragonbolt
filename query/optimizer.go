package query

import (
	"github.com/blacktear23/dragonbolt/txn"
)

type Optimizer struct {
	Query  string
	filter *FilterExec
}

func NewOptimizer(query string) *Optimizer {
	return &Optimizer{
		Query: query,
	}
}

func (o *Optimizer) BuildPlan(t txn.Txn) (Plan, error) {
	err := o.buildFilter()
	if err != nil {
		return nil, err
	}
	return o.buildPlan(t)
}

func (o *Optimizer) buildFilter() error {
	filter, err := BuildExecutor(o.Query)
	if err != nil {
		return err
	}
	o.filter = filter
	return nil
}

func (o *Optimizer) buildPlan(t txn.Txn) (Plan, error) {
	plan := o.doOptimize(t)
	// fmt.Println("[DEBUG]", plan.String())
	err := plan.Init()
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (o *Optimizer) doOptimize(t txn.Txn) Plan {
	expr := o.filter.Ast.Expr
	w := &astWalker{keyEqs: map[string]bool{}}
	w.walkAst(expr)
	// fmt.Printf("[DEBUG] %+v\n", *w)
	if w.numAnd == 0 && w.numOr >= 0 && w.numCall == 0 && w.numKey > 0 && w.numValue == 0 && w.numPM == 0 && w.numReg == 0 && w.numKeyEq == w.numKey && w.numNot == 0 && w.numNeq == 0 {
		// No and only ors no call, all key no prefix match and regexp, key equals == numKey means multi-get
		return NewMultiGetPlan(t, o.filter, getKeys(w.keyEqs))
	}

	if w.numAnd > 0 && w.numOr == 0 && w.numKey > 1 && w.numValue == 0 && w.numKey == w.numEq && w.numNot == 0 && w.numNeq == 0 {
		// All expression is key equals something with and operation
		if len(w.keyEqs) == 1 {
			return NewMultiGetPlan(t, o.filter, getKeys(w.keyEqs))
		}
		return NewEmptyResultPlan(t, o.filter)
	}

	if w.numAnd == 0 && w.numOr == 0 && w.numKey >= 1 && w.numValue == 0 && w.numKey == w.numEq && w.numNot == 0 && w.numNeq == 0 {
		if len(w.keyEqs) == 1 {
			return NewPrefixScanPlan(t, o.filter, getKeys(w.keyEqs)[0])
		}
	}

	if w.numAnd >= 0 && w.numOr == 0 && w.numCall == 0 && w.numKey == 1 && w.numKeyPM == w.numKey && w.numNot == 0 && w.numNeq == 0 {
		// Only one key prefix match and with other or with value field
		return NewPrefixScanPlan(t, o.filter, w.keyPrefixes[0])
	}

	return NewFullScanPlan(t, o.filter)
}

func getKeys(v map[string]bool) []string {
	ret := []string{}
	for k, _ := range v {
		ret = append(ret, k)
	}
	return ret
}

type astWalker struct {
	numAnd      int
	numOr       int
	numKey      int
	numEq       int
	numNeq      int
	numNot      int
	numKeyEq    int
	numValue    int
	numCall     int
	numPM       int
	numKeyPM    int
	numReg      int
	keyPrefixes []string
	keyEqs      map[string]bool
}

func (w *astWalker) walkAst(expr Expression) {
	switch e := expr.(type) {
	case *BinaryOpExpr:
		switch e.Op {
		case And:
			w.numAnd++
		case Or:
			w.numOr++
		case Eq:
			w.numEq++
			if has, val := w.getEqValue(e, KeyKW); has {
				w.numKeyEq++
				w.keyEqs[val] = true
			}
		case NotEq:
			w.numNeq++
		case PrefixMatch:
			w.numPM++
			if has, prefix := w.getPrefixMatchPrefix(e, KeyKW); has {
				w.numKeyPM++
				w.keyPrefixes = append(w.keyPrefixes, prefix)
			}
		case RegExpMatch:
			w.numReg++
		}
		w.walkAst(e.Left)
		w.walkAst(e.Right)
	case *FieldExpr:
		switch e.Field {
		case KeyKW:
			w.numKey++
		case ValueKW:
			w.numValue++
		}
	case *NotExpr:
		w.numNot++
		w.walkAst(e.Right)
	case *FunctionCallExpr:
		w.numCall++
	}
}

func (w *astWalker) getPrefixMatchPrefix(e *BinaryOpExpr, field KVKeyword) (bool, string) {
	var (
		ret        = ""
		has        = false
		matchField = false
	)
	switch l := e.Left.(type) {
	case *StringExpr:
		ret = l.Data
		has = true
	case *FieldExpr:
		if l.Field == field {
			matchField = true
		}
	}

	switch r := e.Right.(type) {
	case *StringExpr:
		if !has {
			ret = r.Data
			has = true
		}
	case *FieldExpr:
		if r.Field == field {
			matchField = true
		}
	}
	if matchField && has {
		return true, ret
	}
	return false, ""
}

func (w *astWalker) getEqValue(e *BinaryOpExpr, field KVKeyword) (bool, string) {
	var (
		ret        = ""
		has        = false
		matchField = false
	)
	switch l := e.Left.(type) {
	case *StringExpr:
		ret = l.Data
		has = true
	case *FieldExpr:
		if l.Field == field {
			matchField = true
		}
	}

	switch r := e.Right.(type) {
	case *StringExpr:
		if !has {
			ret = r.Data
			has = true
		}
	case *FieldExpr:
		if r.Field == field {
			matchField = true
		}
	}
	if matchField && has {
		return true, ret
	}
	return false, ""
}
