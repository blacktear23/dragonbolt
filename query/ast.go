package query

import (
	"fmt"
	"strings"
)

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
	Not         Operator = 3
	Eq          Operator = 4
	NotEq       Operator = 5
	PrefixMatch Operator = 6
	RegExpMatch Operator = 7
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
		Not:         "!",
		PrefixMatch: "^=",
		RegExpMatch: "~=",
	}

	StringToOperator = map[string]Operator{
		"=":  Eq,
		"&":  And,
		"|":  Or,
		"!":  Not,
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
	Expr: BinaryOpExpr {
		Op: "&",
		Left: BinaryOpExpr {
			Op: "^=",
			Left: FieldExpr{Field: KEY},
			Right: StringExpr{Data: "test"},
		},
		Right: BinaryOpExpr {
			Op: "~=",
			Left: FieldExpr{Field: VALUE},
			Right: StringExpr{Data: "test"},
		}
	},
}
*/

var (
	_ Expression = (*BinaryOpExpr)(nil)
	_ Expression = (*FieldExpr)(nil)
	_ Expression = (*StringExpr)(nil)
	_ Expression = (*NotExpr)(nil)
	_ Expression = (*FunctionCallExpr)(nil)
	_ Expression = (*NameExpr)(nil)
)

type Expression interface {
	String() string
	Execute(kv KVPair) (any, error)
}

type WhereStmt struct {
	Expr Expression
}

type BinaryOpExpr struct {
	Op    Operator
	Left  Expression
	Right Expression
}

func (e *BinaryOpExpr) String() string {
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

type NotExpr struct {
	Right Expression
}

func (e *NotExpr) String() string {
	return fmt.Sprintf("!{%s}", e.Right.String())
}

type FunctionCallExpr struct {
	Name Expression
	Args []Expression
}

func (e *FunctionCallExpr) String() string {
	args := make([]string, len(e.Args))
	for i, expr := range e.Args {
		args[i] = expr.String()
	}
	return fmt.Sprintf("[%s]{%s}", e.Name.String(), strings.Join(args, ", "))
}

type NameExpr struct {
	Data string
}

func (e *NameExpr) String() string {
	return fmt.Sprintf("%s", e.Data)
}
