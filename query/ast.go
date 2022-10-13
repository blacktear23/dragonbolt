package query

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
	Eq          Operator = 1
	And         Operator = 2
	Or          Operator = 3
	PrefixMatch Operator = 4
	RegExpMatch Operator = 5
)

var (
	OperatorToString = map[Operator]string{
		Eq:          "=",
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
	Expr: {
		Compares: [
			KVCompareExpr{
				Left: Key,
				Right: "test",
				Op: PrefixMatch,
			},
			KVCompareExpr{
				Left: Value,
				Right: "test",
				Op: RegexpMatch,
			},
		],
		JoinOperators: [
			And,
		],
	},
}
*/

type WhereStmt struct {
	Expr *Expression
}

type Expression struct {
	Compares      []KVCompareExpr
	JoinOperators []Operator
}

type KVCompareExpr struct {
	Op    Operator
	Left  KVKeyword
	Right string
}
