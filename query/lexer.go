package query

import (
	"fmt"
	"strings"
)

type TokenType byte

const (
	WHERE    TokenType = 1
	KEY      TokenType = 2
	VALUE    TokenType = 3
	OPERATOR TokenType = 4
	STRING   TokenType = 5
)

var (
	TokenTypeToString = map[TokenType]string{
		WHERE:    "where",
		KEY:      "key",
		VALUE:    "value",
		OPERATOR: "op",
		STRING:   "str",
	}
)

type Token struct {
	Tp   TokenType
	Data string
	Pos  int
}

func (t *Token) String() string {
	tp := TokenTypeToString[t.Tp]
	return fmt.Sprintf("Tp: %s, Data: %s, Pos: %d", tp, t.Data, t.Pos)
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
		ret         []*Token
		strStart    bool = false
		tokStartPos int
	)
	for i := 0; i < l.Length; i++ {
		char := l.Query[i]
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
		case '~', '^', '=':
			if strStart {
				curr += string(char)
				break
			}
			if token := buildToken(curr, tokStartPos); token != nil {
				ret = append(ret, token)
			}
			curr = ""
			var token *Token = nil
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
		case '&', '|':
			if strStart {
				curr += string(char)
				break
			}
			if token := buildToken(curr, tokStartPos); token != nil {
				ret = append(ret, token)
			}
			token := &Token{
				Tp:   OPERATOR,
				Data: string(char),
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
	case "where":
		token.Tp = WHERE
		return token
	case "key":
		token.Tp = KEY
		return token
	case "value":
		token.Tp = VALUE
		return token
	}
	return nil
}
