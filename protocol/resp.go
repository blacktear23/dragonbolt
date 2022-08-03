package protocol

import (
	"bytes"
	"errors"
	"strconv"
)

var (
	ErrInvalidType        = errors.New("Invalid type")
	ErrInvalidLength      = errors.New("Invalid length")
	ErrInvalidData        = errors.New("Invalid data")
	ErrInvalidArrayLength = errors.New("Invalid array length")
	ErrNotEnoughItems     = errors.New("Not enough items")
	ErrInvalidDictLength  = errors.New("Invalid dict length")
	ErrInvalidDictKeyType = errors.New("Invalid dict key type")

	doubleInf    = []byte("inf")
	doubleNegInf = []byte("-inf")
)

// Parser
type Parser struct {
	pos    int
	length int
	data   []byte
}

func NewParser(data []byte) *Parser {
	return &Parser{
		pos:    0,
		length: len(data),
		data:   data,
	}
}

func (p *Parser) AllParsed() bool {
	return p.pos >= p.length-1
}

func (p *Parser) Next() (any, error) {
	if !p.AllParsed() {
		return p.parseItem()
	}
	return nil, nil
}

func (p *Parser) parseItem() (Encodable, error) {
	for {
		if p.pos > p.length-1 {
			// EOF
			return nil, nil
		}
		curr := p.data[p.pos]
		p.pos += 1
		switch curr {
		case ' ', '\t':
			continue
		case '$':
			return p.parseBlobString()
		case '+':
			return p.parseSimpleString()
		case '-':
			return p.parseSimpleError()
		case ':':
			return p.parseNumber()
		case ',':
			return p.parseDouble()
		case '#':
			return p.parseBoolean()
		case '!':
			return p.parseBlobError()
		case '_':
			if !(p.data[p.pos] == '\r' && p.data[p.pos+1] == '\n') {
				return nil, ErrInvalidData
			}
			return &Null{}, nil
		case '(':
			return p.parseBigNumber()
		case '*':
			return p.parseArray()
		case '~':
			return p.parseSet()
		case '%':
			return p.parseDict()
		case '|':
			return p.parseAttribute()
		default:
			return nil, ErrInvalidType
		}
	}
}

func (p *Parser) parseBlobData() (int, []byte, error) {
	lenStartPos := p.pos
	lenEndPos := lenStartPos
	correct := false
	for ; lenEndPos < p.length-1; lenEndPos++ {
		if p.data[lenEndPos] == '\r' && p.data[lenEndPos+1] == '\n' {
			correct = true
			break
		}
	}
	if !correct {
		return 0, nil, ErrInvalidLength
	}
	lengthData := p.data[lenStartPos:lenEndPos]
	length, err := strconv.Atoi(string(lengthData))
	if err != nil {
		return 0, nil, ErrInvalidLength
	}
	dataStartPos := lenEndPos + 2
	dataEndPos := dataStartPos + length
	if dataEndPos+2 > p.length {
		return 0, nil, ErrInvalidData
	}
	if !(p.data[dataEndPos] == '\r' && p.data[dataEndPos+1] == '\n') {
		return 0, nil, ErrInvalidData
	}
	p.pos = dataEndPos + 2
	return length, p.data[dataStartPos:dataEndPos], nil
}

func (p *Parser) parseSimpleData() ([]byte, error) {
	dataStartPos := p.pos
	dataEndPos := dataStartPos
	correct := false
	for ; dataEndPos < p.length-1; dataEndPos++ {
		if p.data[dataEndPos] == '\r' && p.data[dataEndPos+1] == '\n' {
			correct = true
			break
		}
	}
	if !correct {
		return nil, ErrInvalidData
	}
	p.pos = dataEndPos + 2
	return p.data[dataStartPos:dataEndPos], nil
}

func (p *Parser) parseSimpleString() (*SimpleString, error) {
	data, err := p.parseSimpleData()
	if err != nil {
		return nil, err
	}
	return &SimpleString{
		data: data,
	}, nil
}

func (p *Parser) parseSimpleError() (*SimpleError, error) {
	data, err := p.parseSimpleData()
	if err != nil {
		return nil, err
	}
	return &SimpleError{
		data: data,
	}, nil
}

func (p *Parser) parseBlobString() (*BlobString, error) {
	length, data, err := p.parseBlobData()
	if err != nil {
		return nil, err
	}
	return &BlobString{
		length: length,
		data:   data,
	}, nil
}

func (p *Parser) parseBlobError() (*BlobError, error) {
	length, data, err := p.parseBlobData()
	if err != nil {
		return nil, err
	}
	return &BlobError{
		length: length,
		data:   data,
	}, nil
}

func (p *Parser) parseNumber() (*Number, error) {
	data, err := p.parseSimpleData()
	if err != nil {
		return nil, err
	}
	number, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return nil, ErrInvalidData
	}
	return &Number{
		data: number,
	}, nil
}

func (p *Parser) parseDouble() (*Double, error) {
	data, err := p.parseSimpleData()
	if err != nil {
		return nil, err
	}
	if bytes.Equal(data, doubleInf) {
		return &Double{
			isInf: true,
		}, nil
	} else if bytes.Equal(data, doubleNegInf) {
		return &Double{
			isNegInf: true,
		}, nil
	}
	double, err := strconv.ParseFloat(string(data), 64)
	if err != nil {
		return nil, err
	}
	return &Double{
		data: double,
	}, nil
}

func (p *Parser) parseBigNumber() (*BigNumber, error) {
	data, err := p.parseSimpleData()
	if err != nil {
		return nil, err
	}
	return &BigNumber{
		data: data,
	}, nil
}

func (p *Parser) parseBoolean() (*Boolean, error) {
	pos := p.pos
	if pos+2 > p.length-1 {
		return nil, ErrInvalidData
	}
	if !(p.data[pos+1] == '\r' && p.data[pos+2] == '\n') {
		return nil, ErrInvalidData
	}

	switch p.data[pos] {
	case 't':
		return &Boolean{
			data: true,
		}, nil
	case 'f':
		return &Boolean{
			data: false,
		}, nil
	}
	return nil, ErrInvalidData
}

func (p *Parser) parseArray() (Array, error) {
	data, err := p.parseSimpleData()
	if err != nil {
		return nil, err
	}
	length, err := strconv.Atoi(string(data))
	if err != nil {
		return nil, ErrInvalidArrayLength
	}
	ret := make(Array, length)
	for i := 0; i < length; i++ {
		item, err := p.parseItem()
		if err != nil {
			return nil, err
		}
		if item == nil {
			return nil, ErrNotEnoughItems
		}
		ret[i] = item
	}
	return ret, nil
}

func (p *Parser) parseSet() (Set, error) {
	data, err := p.parseSimpleData()
	if err != nil {
		return nil, err
	}
	length, err := strconv.Atoi(string(data))
	if err != nil {
		return nil, ErrInvalidArrayLength
	}
	ret := make(Set, length)
	for i := 0; i < length; i++ {
		item, err := p.parseItem()
		if err != nil {
			return nil, err
		}
		if item == nil {
			return nil, ErrNotEnoughItems
		}
		ret[i] = item
	}
	return ret, nil
}

func (p *Parser) parseDict() (Dict, error) {
	data, err := p.parseSimpleData()
	if err != nil {
		return nil, err
	}
	length, err := strconv.Atoi(string(data))
	if err != nil {
		return nil, ErrInvalidDictLength
	}
	ret := Dict{}
	for i := 0; i < length; i++ {
		item, err := p.parseItem()
		if err != nil {
			return nil, err
		}
		if item == nil {
			return nil, ErrNotEnoughItems
		}
		key, ok := item.(*SimpleString)
		if !ok {
			return nil, ErrInvalidDictKeyType
		}
		value, err := p.parseItem()
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, ErrNotEnoughItems
		}
		ret[key.String()] = value
	}
	return ret, nil
}

func (p *Parser) parseAttribute() (Dict, error) {
	// Just use parse dict instead
	return p.parseDict()
}
