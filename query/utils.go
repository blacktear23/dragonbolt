package query

func BuildExecutor(query string) (*SelectStmt, *FilterExec, error) {
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		return nil, nil, err
	}
	return expr, &FilterExec{
		Ast: expr.Where,
	}, nil
}

func convertToByteArray(value any) ([]byte, bool) {
	switch ret := value.(type) {
	case []byte:
		return ret, true
	case string:
		return []byte(ret), true
	default:
		return nil, false
	}
}
