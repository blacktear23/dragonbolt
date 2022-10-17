package query

import "fmt"

/*
Query Examples:
	query 'where key ^= "test"'        // key prefix match
	query 'where key ~= "[regexp]"'    // key regexp match
	query 'where value ^= "test"'      // value prefix match
	query 'where value ~= "[regexp]"'  // value regexp match
*/

type KVKeyword byte
type Operator byte

const (
	KeyKW   KVKeyword = 1
	ValueKW KVKeyword = 2

	Unknown     Operator = 0
	And         Operator = 1
	Or          Operator = 2
	Eq          Operator = 3
	NotEq       Operator = 4
	PrefixMatch Operator = 5
	RegExpMatch Operator = 6
)

var (
	KVKeywordToString = map[KVKeyword]string{
		KeyKW:   "KEY",
		ValueKW: "VALUE",
	}

	OperatorToString = map[Operator]string{
		Eq:          "=",
		NotEq:       "!=",
		And:         "&",
		Or:          "|",
		PrefixMatch: "^=",
		RegExpMatch: "~=",
	}

	StringToOperator = map[string]Operator{
		"=":  Eq,
		"&":  And,
		"|":  Or,
		"^=": PrefixMatch,
		"~=": RegExpMatch,
		"!=": NotEq,
	}
)

func BuildOp(op string) (Operator, error) {
	ret, have := StringToOperator[op]
	if !have {
		return Unknown, ErrSyntaxUnknownOperator
	}
	return ret, nil
}

/*
query: where key ^= "test" & value ~= "test"
WhereStmt {
	Expr: CompareExpr {
		Op: "&",
		Left: CompareExpr {
			Op: "^=",
			Left: FieldExpr{Field: KEY},
			Right: StringExpr{Data: "test"},
		},
		Right: CompareExpr {
			Op: "~=",
			Left: FieldExpr{Field: VALUE},
			Right: StringExpr{Data: "test"},
		}
	},
}
*/

var (
	_ Expression = (*CompareExpr)(nil)
	_ Expression = (*FieldExpr)(nil)
	_ Expression = (*StringExpr)(nil)
)

type Expression interface {
	String() string
	Execute(kv KVPair) (any, error)
}

type WhereStmt struct {
	Expr Expression
}

type CompareExpr struct {
	Op    Operator
	Left  Expression
	Right Expression
}

func (e *CompareExpr) String() string {
	op := OperatorToString[e.Op]
	return fmt.Sprintf("{%s %s %s}", e.Left.String(), op, e.Right.String())
}

type FieldExpr struct {
	Field KVKeyword
}

func (e *FieldExpr) String() string {
	return fmt.Sprintf("%s", KVKeywordToString[e.Field])
}

type StringExpr struct {
	Data string
}

func (e *StringExpr) String() string {
	return fmt.Sprintf("`%s`", e.Data)
}
