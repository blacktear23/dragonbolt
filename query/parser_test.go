package query

import (
	"fmt"
	"testing"
)

func TestParser1(t *testing.T) {
	query := "where key = 'test' & value = 'value'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Expr.String())
}

func TestParser2(t *testing.T) {
	query := "where key ^= 'test'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Expr.String())
}

func TestParser3(t *testing.T) {
	query := "where key ^= 'test' value = 'xxx'"
	p := NewParser(query)
	_, err := p.Parse()
	if err == nil {
		t.Fatal("Should get syntax error")
	}
	fmt.Printf("%+v\n", err)
}

func TestParser4(t *testing.T) {
	query := "where (key ^= 'test' | key ^= 'bar') & value = 'xxx'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Expr.String())
}

func TestParser5(t *testing.T) {
	query := "where (key ^= 'test' | (key ^= 'bar' & key ^= 'foo')) & value = 'xxx'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Expr.String())
}
