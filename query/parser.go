package query

import (
	"errors"
)

var (
	ErrSyntaxStartWhere               = errors.New("Syntax Error: not start with `where`")
	ErrSyntaxNoOperator               = errors.New("Syntax Error: no operator follow the token")
	ErrSyntaxUnknownOperator          = errors.New("Syntax Error: unknown operator")
	ErrSyntaxInvalidSyntax            = errors.New("Syntax Error: invalid syntax")
	ErrSyntaxWrongOperatorPosition    = errors.New("Syntax Error: wrong operator | or & position")
	ErrSyntaxOperatorShouldNotHere    = errors.New("Syntax Error: cannot use operator here")
	ErrSyntaxUnsupportCompareOperator = errors.New("Syntax Error: unsupport compare operator")
)

type Parser struct {
	Query string
	lex   *Lexer
}

func NewParser(query string) *Parser {
	return &Parser{
		Query: query,
		lex:   NewLexer(query),
	}
}

func (p *Parser) Parse() (*WhereStmt, error) {
	toks := p.lex.Split()
	ntoks := len(toks)
	if ntoks == 0 {
		return nil, ErrSyntaxStartWhere
	}
	if toks[0].Tp != WHERE {
		return nil, ErrSyntaxStartWhere
	}

	expr := &Expression{
		Compares:      []KVCompareExpr{},
		JoinOperators: []Operator{},
	}

	idx := 1
	prevIsCompare := false
	for idx < len(toks) {
		tok := toks[idx]
		switch tok.Tp {
		case KEY, VALUE:
			if prevIsCompare {
				return nil, ErrSyntaxInvalidSyntax
			}
			if idx+2 >= ntoks {
				return nil, ErrSyntaxNoOperator
			}
			left := KeyKW
			if tok.Tp == VALUE {
				left = ValueKW
			}
			op, err := BuildOp(toks[idx+1].Data)
			if err != nil {
				return nil, err
			}
			switch op {
			case And, Or:
				return nil, ErrSyntaxUnsupportCompareOperator
			}
			right := toks[idx+2].Data
			cmpr := KVCompareExpr{
				Op:    op,
				Left:  left,
				Right: right,
			}
			expr.Compares = append(expr.Compares, cmpr)
			idx += 3
			prevIsCompare = true
		case OPERATOR:
			if prevIsCompare {
				prevIsCompare = false
			} else {
				return nil, ErrSyntaxWrongOperatorPosition
			}
			switch tok.Data {
			case "|", "&":
				op, _ := BuildOp(tok.Data)
				expr.JoinOperators = append(expr.JoinOperators, op)
			default:
				return nil, ErrSyntaxOperatorShouldNotHere
			}
			idx += 1
		default:
			return nil, ErrSyntaxInvalidSyntax
		}
	}

	return &WhereStmt{
		Expr: expr,
	}, nil
}
