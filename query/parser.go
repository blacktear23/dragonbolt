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
	Query   string
	lex     *Lexer
	toks    []*Token
	pos     int
	numToks int
}

func NewParser(query string) *Parser {
	lex := NewLexer(query)
	toks := lex.Split()
	return &Parser{
		Query:   query,
		lex:     lex,
		toks:    toks,
		pos:     0,
		numToks: len(toks),
	}
}

func (p *Parser) next() *Token {
	if p.pos >= p.numToks {
		return nil
	}
	ret := p.toks[p.pos]
	p.pos += 1
	return ret
}

func (p *Parser) hasNext() bool {
	return p.pos < p.numToks-1
}

func (p *Parser) back() {
	p.pos -= 1
}

func (p *Parser) parseBlock(endWithBrace bool, i int) (Expression, error) {
	var (
		expr Expression = nil
		err  error      = nil
	)

	for {
		tok := p.next()
		if tok == nil && expr == nil {
			return nil, ErrSyntaxInvalidSyntax
		}
		if tok == nil {
			break
		}
		switch tok.Tp {
		case KEY:
			if expr != nil {
				return nil, ErrSyntaxInvalidSyntax
			}
			expr = &FieldExpr{
				Field: KeyKW,
			}
			// fmt.Println(i, expr)
		case VALUE:
			if expr != nil {
				return nil, ErrSyntaxInvalidSyntax
			}
			expr = &FieldExpr{
				Field: ValueKW,
			}
			// fmt.Println(i, expr)
		case STRING:
			if expr != nil {
				return nil, ErrSyntaxInvalidSyntax
			}
			expr = &StringExpr{
				Data: tok.Data,
			}
			// fmt.Println(i, "STRING", expr)
		case OPERATOR:
			switch tok.Data {
			case "|", "&":
				// fmt.Println(i, "OP", expr)
				right, err := p.parseBlock(true, i+1)
				if err != nil {
					return nil, err
				}
				op, _ := BuildOp(tok.Data)
				expr = &CompareExpr{
					Op:    op,
					Left:  expr,
					Right: right,
				}
				// fmt.Println(i, "~OP", expr)
				if endWithBrace {
					return expr, nil
				}
			case "^=", "~=", "!=", "=":
				// fmt.Println(i, "OPSTR", expr)
				right, err := p.parseRight(i)
				if err != nil {
					return nil, err
				}
				op, _ := BuildOp(tok.Data)
				expr = &CompareExpr{
					Op:    op,
					Left:  expr,
					Right: right,
				}
				// fmt.Println(i, "~OPSTR", expr)
			case "!":
				tok = p.next()
				if tok.Tp != LBRACE {
					return nil, errors.New("! operator should follow (")
				}
				if expr != nil {
					return nil, errors.New("! operator should not have left expression")
				}
				right, err := p.parseBlock(true, i+1)
				if err != nil {
					return nil, err
				}
				expr = &NotExpr{
					Right: right,
				}
				if endWithBrace {
					return expr, nil
				}
			}
		case LBRACE:
			// fmt.Println(i, "LBRACE", expr)
			expr, err = p.parseBlock(true, i+1)
			// fmt.Println(i, "~LBRACE", expr)
			if err != nil {
				return nil, err
			}
		case RBRACE:
			// fmt.Println(i, endWithBrace, "RBRACE", expr)
			if endWithBrace {
				return expr, nil
			}
			break
		default:
			return nil, ErrSyntaxInvalidSyntax
		}
	}
	return expr, nil
}

func (p *Parser) parseRight(i int) (Expression, error) {
	tok := p.next()
	switch tok.Tp {
	case KEY:
		return &FieldExpr{
			Field: KeyKW,
		}, nil
	case VALUE:
		return &FieldExpr{
			Field: ValueKW,
		}, nil
	case STRING:
		return &StringExpr{
			Data: tok.Data,
		}, nil
	default:
		return nil, ErrSyntaxInvalidSyntax
	}
}

func (p *Parser) Parse() (*WhereStmt, error) {
	if p.numToks == 0 {
		return nil, ErrSyntaxStartWhere
	}

	curr := p.next()
	if curr == nil || curr.Tp != WHERE {
		return nil, ErrSyntaxStartWhere
	}

	expr, err := p.parseBlock(false, 0)
	if err != nil {
		return nil, err
	}

	return &WhereStmt{
		Expr: expr,
	}, nil
}
