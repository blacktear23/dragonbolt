package query

import (
	"fmt"
	"strings"
)

type TokenType byte

const (
	SELECT   TokenType = 1
	WHERE    TokenType = 2
	KEY      TokenType = 3
	VALUE    TokenType = 4
	OPERATOR TokenType = 5
	STRING   TokenType = 6
	LBRACE   TokenType = 7
	RBRACE   TokenType = 8
	NAME     TokenType = 9
	SEP      TokenType = 10
)

var (
	TokenTypeToString = map[TokenType]string{
		SELECT:   "select",
		WHERE:    "where",
		KEY:      "key",
		VALUE:    "value",
		OPERATOR: "op",
		STRING:   "str",
		LBRACE:   "(",
		RBRACE:   ")",
		NAME:     "name",
		SEP:      "SEP",
	}
)

const (
	LowestPrec  = 0 // non-operators
	UnaryPrec   = 6
	HighestPrec = 7
)

type Token struct {
	Tp   TokenType
	Data string
	Pos  int
}

func (t *Token) String() string {
	tp := TokenTypeToString[t.Tp]
	return fmt.Sprintf("Tp: %6s  Data: %10s  Pos: %d", tp, t.Data, t.Pos)
}

func (t *Token) Precedence() int {
	switch t.Tp {
	case OPERATOR:
		switch t.Data {
		case "|":
			return 1
		case "&":
			return 2
		case "=", "!=", "^=", "~=":
			return 3
		case "+", "-":
			return 4
		case "*", "/":
			return 5
		}
	}
	return LowestPrec
}

type Lexer struct {
	Query  string
	Length int
}

func NewLexer(query string) *Lexer {
	return &Lexer{
		Query:  query,
		Length: len(query),
	}
}

func (l *Lexer) Split() []*Token {
	var (
		curr        string
		prev        byte
		next        byte
		ret         []*Token
		strStart    bool = false
		tokStartPos int
	)
	for i := 0; i < l.Length; i++ {
		char := l.Query[i]
		if i < l.Length-1 {
			next = l.Query[i+1]
		} else {
			next = 0
		}
		switch char {
		case ' ':
			if strStart {
				curr += string(char)
				break
			}
			if token := buildToken(curr, tokStartPos); token != nil {
				ret = append(ret, token)
			}
			curr = ""
			tokStartPos = i + 1
		case '"', '\'':
			if !strStart {
				strStart = true
				tokStartPos = i
			} else {
				strStart = false
				token := &Token{
					Tp:   STRING,
					Data: curr,
					Pos:  tokStartPos,
				}
				ret = append(ret, token)
				curr = ""
			}
		case '~', '^', '=', '!', '*':
			if strStart {
				curr += string(char)
				break
			}
			if token := buildToken(curr, tokStartPos); token != nil {
				ret = append(ret, token)
			}
			curr = ""
			var token *Token = nil

			if char == '!' && next != '=' {
				token = &Token{
					Tp:   OPERATOR,
					Data: "!",
					Pos:  i,
				}
				ret = append(ret, token)
				tokStartPos = i + 1
				break
			}

			if char == '*' && next != '=' {
				token = &Token{
					Tp:   OPERATOR,
					Data: "*",
					Pos:  i,
				}
				ret = append(ret, token)
				tokStartPos = i + 1
				break
			}

			if char == '=' {
				switch prev {
				case '^':
					token = &Token{
						Tp:   OPERATOR,
						Data: "^=",
						Pos:  i - 1,
					}
				case '~':
					token = &Token{
						Tp:   OPERATOR,
						Data: "~=",
						Pos:  i - 1,
					}
				case '!':
					token = &Token{
						Tp:   OPERATOR,
						Data: "!=",
						Pos:  i - 1,
					}
				default:
					token = &Token{
						Tp:   OPERATOR,
						Data: "=",
						Pos:  i,
					}
				}
				if token != nil {
					ret = append(ret, token)
				}
			}
			tokStartPos = i + 1
		case '&', '|', '(', ')':
			if strStart {
				curr += string(char)
				break
			}
			token := buildToken(curr, tokStartPos)
			if token != nil {
				ret = append(ret, token)
			}
			if char == '(' {
				token = &Token{
					Tp:   LBRACE,
					Data: string(char),
					Pos:  i,
				}
			} else if char == ')' {
				token = &Token{
					Tp:   RBRACE,
					Data: string(char),
					Pos:  i,
				}
			} else {
				token = &Token{
					Tp:   OPERATOR,
					Data: string(char),
					Pos:  i,
				}
			}
			ret = append(ret, token)
			curr = ""
			tokStartPos = i + 1
		case ',':
			if strStart {
				curr += string(char)
				break
			}
			token := buildToken(curr, tokStartPos)
			if token != nil {
				ret = append(ret, token)
			}
			token = &Token{
				Tp:   SEP,
				Data: ",",
				Pos:  i,
			}
			ret = append(ret, token)
			curr = ""
			tokStartPos = i + 1
		default:
			curr += string(char)
		}
		prev = char
	}
	if len(curr) > 0 {
		if token := buildToken(curr, tokStartPos); token != nil {
			ret = append(ret, token)
		}
	}
	return ret
}

func buildToken(curr string, pos int) *Token {
	curr = strings.ToLower(strings.TrimSpace(curr))
	if len(curr) == 0 {
		return nil
	}
	token := &Token{
		Data: curr,
		Pos:  pos,
	}
	switch curr {
	case "select":
		token.Tp = SELECT
		return token
	case "where":
		token.Tp = WHERE
		return token
	case "key":
		token.Tp = KEY
		return token
	case "value":
		token.Tp = VALUE
		return token
	default:
		token.Tp = NAME
		return token
	}
	return nil
}
