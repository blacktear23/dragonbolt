package query

import (
	"fmt"
	"testing"
)

func TestLexer1(t *testing.T) {
	query := "where key = 'test' & value = 'value'"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer2(t *testing.T) {
	query := "where key ^= 'test' | key ~= 'value' & value = 'test'"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer3(t *testing.T) {
	query := "where key^='test'|key~='value'&value='test' "
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}
