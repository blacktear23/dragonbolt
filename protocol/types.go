package protocol

import (
	"fmt"
	"strconv"
)

var (
	endLine = []byte("\r\n")

	_ Encodable = (Encodable)(&BlobString{})
	_ Encodable = (Encodable)(&BlobError{})
	_ Encodable = (Encodable)(&SimpleString{})
	_ Encodable = (Encodable)(&SimpleError{})
	_ Encodable = (Encodable)(&Null{})
	_ Encodable = (Encodable)(&Number{})
	_ Encodable = (Encodable)(&Double{})
	_ Encodable = (Encodable)(&Boolean{})
	_ Encodable = (Encodable)(&BigNumber{})
	_ Encodable = (Encodable)(Array{})
	_ Encodable = (Encodable)(Dict{})
	_ Encodable = (Encodable)(Set{})

	_ Savable = (Savable)(&BlobString{})
	_ Savable = (Savable)(&SimpleString{})
	_ Savable = (Savable)(&Number{})
	_ Savable = (Savable)(&Double{})
	_ Savable = (Savable)(&Boolean{})
	_ Savable = (Savable)(&BigNumber{})
)

type Encodable interface {
	Encode() []byte
}

type Savable interface {
	Bytes() []byte
}

// Blob String
type BlobString struct {
	length int
	data   []byte
}

func NewBlobString(data []byte) *BlobString {
	return &BlobString{
		length: len(data),
		data:   data,
	}
}

func (bs *BlobString) GetData() []byte {
	return bs.data
}

func (bs *BlobString) String() string {
	return string(bs.data)
}

func (bs *BlobString) Bytes() []byte {
	return bs.data
}

func (bs *BlobString) Encode() []byte {
	ret := make([]byte, 1, bs.length+16)
	ret[0] = '$'
	ret = append(ret, []byte(strconv.Itoa(bs.length))...)
	ret = append(ret, endLine...)
	ret = append(ret, bs.data...)
	ret = append(ret, endLine...)
	return ret
}

// Blob Error
type BlobError struct {
	length int
	data   []byte
}

func NewBlobError(err string) *BlobError {
	data := []byte(err)
	return &BlobError{
		length: len(data),
		data:   data,
	}
}

func (be *BlobError) String() string {
	return string(be.data)
}

func (be *BlobError) Encode() []byte {
	ret := make([]byte, 1, be.length+16)
	ret[0] = '!'
	ret = append(ret, []byte(strconv.Itoa(be.length))...)
	ret = append(ret, endLine...)
	ret = append(ret, be.data...)
	ret = append(ret, endLine...)
	return ret
}

// Simple String
type SimpleString struct {
	data []byte
}

func NewSimpleString(str string) *SimpleString {
	data := []byte(str)
	return &SimpleString{
		data: data,
	}
}

func NewSimpleStringf(format string, args ...any) *SimpleString {
	str := fmt.Sprintf(format, args...)
	return NewSimpleString(str)
}

func (ss *SimpleString) Bytes() []byte {
	return ss.data
}

func (ss *SimpleString) String() string {
	return string(ss.data)
}

func (ss *SimpleString) Encode() []byte {
	ret := make([]byte, 1, len(ss.data)+2)
	ret[0] = '+'
	ret = append(ret, ss.data...)
	ret = append(ret, endLine...)
	return ret
}

// Simple Error
type SimpleError struct {
	data []byte
}

func NewSimpleError(err string) *SimpleError {
	data := []byte(err)
	return &SimpleError{
		data: data,
	}
}

func NewSimpleErrorf(format string, args ...any) *SimpleError {
	err := fmt.Sprintf(format, args...)
	return NewSimpleError(err)
}

func (se *SimpleError) String() string {
	return string(se.data)
}

func (se *SimpleError) Encode() []byte {
	ret := make([]byte, 1, len(se.data)+2)
	ret[0] = '-'
	ret = append(ret, se.data...)
	ret = append(ret, endLine...)
	return ret
}

// Null
type Null struct{}

func NewNull() *Null {
	return &Null{}
}

func (n *Null) Encode() []byte {
	return []byte("_\r\n")
}

// Number
type Number struct {
	data int64
}

func NewNumber(num int64) *Number {
	return &Number{
		data: num,
	}
}

func NewNumberUint(num uint64) *Number {
	return &Number{
		data: int64(num),
	}
}

func (n *Number) GetNumber() int64 {
	return n.data
}

func (n *Number) Encode() []byte {
	ret := make([]byte, 1, 16)
	ret[0] = ':'
	ret = append(ret, []byte(strconv.FormatInt(n.data, 10))...)
	ret = append(ret, endLine...)
	return ret
}

func (n *Number) Bytes() []byte {
	return []byte(strconv.FormatInt(n.data, 10))
}

func (n *Number) String() string {
	return strconv.FormatInt(n.data, 10)
}

// Double
type Double struct {
	data     float64
	isInf    bool
	isNegInf bool
}

func NewDouble(num float64) *Double {
	return &Double{
		data: num,
	}
}

func NewInf() *Double {
	return &Double{
		isInf: true,
	}
}

func NewNegInf() *Double {
	return &Double{
		isNegInf: true,
	}
}

func (n *Double) Bytes() []byte {
	return []byte(n.String())
}

func (n *Double) GetDouble() float64 {
	return n.data
}

func (n *Double) IsInf() (bool, bool) {
	return n.isInf, n.isNegInf
}

func (n *Double) String() string {
	if n.isInf {
		return "Inf"
	} else if n.isNegInf {
		return "-Inf"
	}
	return strconv.FormatFloat(n.data, 'f', -1, 64)
}

func (n *Double) Encode() []byte {
	if n.isInf {
		return []byte(",inf\r\n")
	} else if n.isNegInf {
		return []byte(",-inf\r\n")
	}
	ret := make([]byte, 1, 16)
	ret[0] = ','
	ret = append(ret, []byte(n.String())...)
	ret = append(ret, endLine...)
	return ret
}

// Boolean
type Boolean struct {
	data bool
}

func NewBoolean(val bool) *Boolean {
	return &Boolean{
		data: val,
	}
}

func (b *Boolean) GetBool() bool {
	return b.data
}

func (b *Boolean) Bytes() []byte {
	if b.data {
		return []byte("true")
	}
	return []byte("false")
}

func (b *Boolean) Encode() []byte {
	switch b.data {
	case true:
		return []byte("#t\r\n")
	case false:
		return []byte("#f\r\n")
	}
	return nil
}

// Big Number
type BigNumber struct {
	data []byte
}

func NewBigNumber(num string) *BigNumber {
	return &BigNumber{
		data: []byte(num),
	}
}

func (bn *BigNumber) String() string {
	return string(bn.data)
}

func (bn *BigNumber) Bytes() []byte {
	return bn.data
}

func (bn *BigNumber) Encode() []byte {
	ret := make([]byte, 1, len(bn.data))
	ret[0] = '('
	ret = append(ret, bn.data...)
	ret = append(ret, endLine...)
	return ret
}

// Array
type Array []Encodable

func (a Array) Encode() []byte {
	ret := make([]byte, 1, 16)
	length := len(a)
	ret[0] = '*'
	ret = append(ret, []byte(strconv.Itoa(length))...)
	ret = append(ret, endLine...)
	for _, item := range a {
		ret = append(ret, item.Encode()...)
	}
	return ret
}

// Dict
type Dict map[string]Encodable

func (d Dict) Encode() []byte {
	ret := make([]byte, 1, 16)
	length := len(d)
	ret[0] = '%'
	ret = append(ret, []byte(strconv.Itoa(length))...)
	ret = append(ret, endLine...)
	for key, item := range d {
		// Encode key
		ret = append(ret, '+')
		ret = append(ret, []byte(key)...)
		ret = append(ret, endLine...)
		// Encode value
		ret = append(ret, item.Encode()...)
	}
	return ret
}

// Set
type Set []Encodable

func (s Set) Encode() []byte {
	ret := make([]byte, 1, 16)
	length := len(s)
	ret[0] = '~'
	ret = append(ret, []byte(strconv.Itoa(length))...)
	ret = append(ret, endLine...)
	for _, item := range s {
		ret = append(ret, item.Encode()...)
	}
	return ret
}
