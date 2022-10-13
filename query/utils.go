package query

func BuildExecutor(query string) (*FilterExec, error) {
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		return nil, err
	}
	return &FilterExec{
		Ast: expr,
	}, nil
}
