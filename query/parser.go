package query

import (
	"errors"
	"fmt"
)

var (
	ErrSyntaxStartWhere = errors.New("Syntax Error: not start with `where`")
)

const MaxNestLevel = 1e5

type Parser struct {
	Query   string
	lex     *Lexer
	toks    []*Token
	tok     *Token
	pos     int
	numToks int
	nestLev int
	exprLev int
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
		nestLev: 0,
		exprLev: 0,
	}
}

func (p *Parser) incNestLev() error {
	p.nestLev++
	if p.nestLev > MaxNestLevel {
		return errors.New("exceed max nesting depth")
	}
	return nil
}

func (p *Parser) decNestLev() {
	p.nestLev--
}

func (p *Parser) next() *Token {
	if p.pos >= p.numToks {
		p.tok = nil
		return nil
	}
	p.tok = p.toks[p.pos]
	p.pos += 1
	return p.tok
}

func (p *Parser) expect(tok *Token) error {
	if p.tok == nil {
		return fmt.Errorf("Expect token %s but got EOF", tok.Data)
	}
	if p.tok.Tp != tok.Tp {
		return fmt.Errorf("Expect token %s but got %s", tok.Data, p.tok.Data)
	}
	p.next()
	return nil
}

func (p *Parser) tokPrec() (*Token, int) {
	tok := p.tok
	if tok == nil {
		return nil, LowestPrec
	}
	return tok, tok.Precedence()
}

func (p *Parser) expectOp() (*Token, error) {
	if p.tok == nil {
		return nil, nil
	}
	tp := p.tok.Tp
	switch tp {
	case OPERATOR:
		return p.tok, nil
	}
	return nil, errors.New("Expect operator but got not operator")
}

func (p *Parser) parseExpr() (Expression, error) {
	return p.parseBinaryExpr(nil, LowestPrec+1)
}

func (p *Parser) parseBinaryExpr(x Expression, prec1 int) (Expression, error) {
	var err error
	if x == nil {
		x, err = p.parseUnaryExpr()
		if err != nil {
			return nil, err
		}
	}
	var n int
	defer func() {
		p.nestLev -= n
	}()
	for n = 1; ; n++ {
		err = p.incNestLev()
		if err != nil {
			return nil, err
		}

		opTok, oprec := p.tokPrec()
		if oprec < prec1 {
			return x, nil
		}
		if opTok == nil {
			return x, nil
		}
		err = p.expect(opTok)
		if err != nil {
			return nil, err
		}
		y, err := p.parseBinaryExpr(nil, oprec+1)
		if err != nil {
			return nil, err
		}
		op, err := BuildOp(opTok.Data)
		if err != nil {
			return nil, err
		}
		x = &BinaryOpExpr{Op: op, Left: x, Right: y}
	}
}

func (p *Parser) parseUnaryExpr() (Expression, error) {
	p.incNestLev()
	defer func() {
		p.decNestLev()
	}()
	switch p.tok.Tp {
	case OPERATOR:
		switch p.tok.Data {
		case "!":
			p.next()
			x, err := p.parseUnaryExpr()
			if err != nil {
				return nil, err
			}
			return &NotExpr{Right: x}, nil
		}
	}
	return p.parsePrimaryExpr(nil)
}

func (p *Parser) parseFuncCall(fun Expression) (Expression, error) {
	err := p.expect(&Token{Tp: LBRACE, Data: "("})
	if err != nil {
		return nil, err
	}
	p.exprLev++
	var list []Expression
	for p.tok != nil && p.tok.Tp != RBRACE {
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		list = append(list, arg)
		if p.tok != nil && p.tok.Tp == RBRACE {
			break
		}
		p.next()
	}
	p.exprLev--
	err = p.expect(&Token{Tp: RBRACE, Data: ")"})
	if err != nil {
		return nil, err
	}
	return &FunctionCallExpr{Name: fun, Args: list}, nil
}

func (p *Parser) parsePrimaryExpr(x Expression) (Expression, error) {
	var err error
	if x == nil {
		x, err = p.parseOperand()
		if err != nil {
			return nil, err
		}
	}

	var n int
	defer func() {
		p.nestLev -= n
	}()

	for n = 1; ; n++ {
		p.incNestLev()
		if p.tok == nil {
			return x, nil
		}
		switch p.tok.Tp {
		case LBRACE:
			x, err = p.parseFuncCall(x)
			if err != nil {
				return nil, err
			}
		default:
			return x, nil
		}
	}
}

func (p *Parser) parseOperand() (Expression, error) {
	switch p.tok.Tp {
	case KEY:
		x := &FieldExpr{Field: KeyKW}
		p.next()
		return x, nil
	case VALUE:
		x := &FieldExpr{Field: ValueKW}
		p.next()
		return x, nil
	case STRING:
		x := &StringExpr{Data: p.tok.Data}
		p.next()
		return x, nil
	case LBRACE:
		p.next()
		p.exprLev++
		x, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		p.exprLev--
		err = p.expect(&Token{Tp: RBRACE, Data: ")"})
		if err != nil {
			return nil, err
		}
		return x, nil
	case NAME:
		x := &NameExpr{Data: p.tok.Data}
		p.next()
		return x, nil
	}
	return nil, errors.New("Bad Expression")
}

func (p *Parser) Parse() (*WhereStmt, error) {
	if p.numToks == 0 {
		return nil, ErrSyntaxStartWhere
	}
	p.next()
	if p.tok == nil || p.tok.Tp != WHERE {
		return nil, ErrSyntaxStartWhere
	}
	p.next()

	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.next() != nil {
		return nil, errors.New("Syntax error missing operator")
	}

	// Check syntax
	err = expr.Check()
	if err != nil {
		return nil, err
	}
	return &WhereStmt{
		Expr: expr,
	}, nil
}
