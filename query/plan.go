package query

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/blacktear23/dragonbolt/txn"
)

type Plan interface {
	String() string
	Init() error
	Next() (key []byte, value []byte, err error)
}

var (
	_ Plan = (*FullScanPlan)(nil)
	_ Plan = (*EmptyResultPlan)(nil)
	_ Plan = (*PrefixScanPlan)(nil)
	_ Plan = (*MultiGetPlan)(nil)
	_ Plan = (*LimitPlan)(nil)
)

type FullScanPlan struct {
	Txn    txn.Txn
	Filter *FilterExec
	iter   txn.Cursor
}

func NewFullScanPlan(t txn.Txn, f *FilterExec) Plan {
	return &FullScanPlan{
		Txn:    t,
		Filter: f,
	}
}

func (p *FullScanPlan) String() string {
	return "FullScanPlan"
}

func (p *FullScanPlan) Init() (err error) {
	p.iter, err = p.Txn.Cursor()
	if err != nil {
		return err
	}
	return p.iter.Seek([]byte{})
}

func (p *FullScanPlan) Next() ([]byte, []byte, error) {
	for {
		key, val, err := p.iter.Next()
		if err != nil {
			return nil, nil, err
		}
		if key == nil {
			break
		}
		ok, err := p.Filter.Filter(NewKVP(key, val))
		if err != nil {
			return nil, nil, err
		}
		if ok {
			return key, val, nil
		}
	}
	return nil, nil, nil
}

type EmptyResultPlan struct {
	Txn txn.Txn
}

func NewEmptyResultPlan(t txn.Txn, f *FilterExec) Plan {
	return &EmptyResultPlan{
		Txn: t,
	}
}

func (p *EmptyResultPlan) Init() error {
	return nil
}

func (p *EmptyResultPlan) Next() ([]byte, []byte, error) {
	return nil, nil, nil
}

func (p *EmptyResultPlan) String() string {
	return "EmptyResultPlan"
}

type PrefixScanPlan struct {
	Txn    txn.Txn
	Filter *FilterExec
	Prefix string
	iter   txn.Cursor
}

func NewPrefixScanPlan(t txn.Txn, f *FilterExec, p string) Plan {
	return &PrefixScanPlan{
		Txn:    t,
		Filter: f,
		Prefix: p,
	}
}

func (p *PrefixScanPlan) Init() (err error) {
	p.iter, err = p.Txn.Cursor()
	if err != nil {
		return err
	}
	return p.iter.Seek([]byte(p.Prefix))
}

func (p *PrefixScanPlan) Next() ([]byte, []byte, error) {
	pb := []byte(p.Prefix)
	for {
		key, val, err := p.iter.Next()
		if err != nil {
			return nil, nil, err
		}
		if key == nil {
			break
		}
		if !bytes.HasPrefix(key, pb) {
			break
		}
		ok, err := p.Filter.Filter(NewKVP(key, val))
		if err != nil {
			return nil, nil, err
		}
		if ok {
			return key, val, nil
		}
	}
	return nil, nil, nil
}

func (p *PrefixScanPlan) String() string {
	return fmt.Sprintf("PrefixScanPlan{Prefix = '%s'}", p.Prefix)
}

type MultiGetPlan struct {
	Txn     txn.Txn
	Filter  *FilterExec
	Keys    []string
	numKeys int
	idx     int
}

func NewMultiGetPlan(t txn.Txn, f *FilterExec, keys []string) Plan {
	return &MultiGetPlan{
		Txn:     t,
		Filter:  f,
		Keys:    keys,
		idx:     0,
		numKeys: len(keys),
	}
}

func (p *MultiGetPlan) Init() error {
	return nil
}

func (p *MultiGetPlan) Next() ([]byte, []byte, error) {
	for {
		if p.idx >= p.numKeys {
			break
		}
		key := []byte(p.Keys[p.idx])
		p.idx++
		val, err := p.Txn.Get(key)
		if err != nil {
			return nil, nil, err
		}
		if val == nil {
			// No Value
			continue
		}
		ok, err := p.Filter.Filter(NewKVP(key, val))
		if err != nil {
			return nil, nil, err
		}
		if ok {
			return key, val, nil
		}
	}
	return nil, nil, nil
}

func (p *MultiGetPlan) String() string {
	keys := strings.Join(p.Keys, ", ")
	return fmt.Sprintf("MultiGetPlan{Keys = %s}", keys)
}

type LimitPlan struct {
	Txn       txn.Txn
	Start     int
	Count     int
	current   int
	skips     int
	ChildPlan Plan
}

func (p *LimitPlan) Init() error {
	p.current = 0
	p.skips = 0
	return p.ChildPlan.Init()
}

func (p *LimitPlan) Next() ([]byte, []byte, error) {
	for p.skips < p.Start {
		k, v, err := p.ChildPlan.Next()
		if err != nil {
			return nil, nil, err
		}
		if k == nil && v == nil && err == nil {
			return nil, nil, nil
		}
		p.skips++
	}
	if p.current >= p.Count {
		return nil, nil, nil
	}
	k, v, err := p.ChildPlan.Next()
	if err != nil {
		return nil, nil, err
	}
	if k == nil && v == nil && err == nil {
		return nil, nil, nil
	}

	p.current++
	return k, v, nil

}

func (p *LimitPlan) String() string {
	return fmt.Sprintf("LimitPlan{Start = %d, Count = %d} -> %s", p.Start, p.Count, p.ChildPlan.String())
}

type ProjectionPlan struct {
	Txn       txn.Txn
	ChildPlan Plan
	AllFields bool
	Fields    []Expression
}

type Column []byte

func (p *ProjectionPlan) Next() ([]Column, error) {
	k, v, err := p.ChildPlan.Next()
	if err != nil {
		return nil, err
	}
	if k == nil && v == nil && err == nil {
		return nil, nil
	}
	if p.AllFields {
		return []Column{k, v}, nil
	}
	return p.processProjection(k, v)
}

func (p *ProjectionPlan) processProjection(key []byte, value []byte) ([]Column, error) {
	nFields := len(p.Fields)
	ret := make([]Column, nFields)
	kvp := NewKVP(key, value)
	for i := 0; i < nFields; i++ {
		result, err := p.Fields[i].Execute(kvp)
		if err != nil {
			return nil, err
		}
		switch value := result.(type) {
		case bool:
			if value {
				ret[i] = []byte("true")
			} else {
				ret[i] = []byte("false")
			}
		case []byte:
			ret[i] = value
		case string:
			ret[i] = []byte(value)
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			ret[i] = []byte(fmt.Sprintf("%d", value))
		case float32, float64:
			ret[i] = []byte(fmt.Sprintf("%f", value))
		default:
			if value == nil {
				ret[i] = nil
				break
			}
			return nil, errors.New("Expression result type not support")
		}
	}
	return ret, nil
}

func (p *ProjectionPlan) String() string {
	fields := []string{}
	if p.AllFields {
		fields = append(fields, "*")
	} else {
		for _, f := range p.Fields {
			fields = append(fields, f.String())
		}
	}
	ret := fmt.Sprintf("ProjectionPlan{Fields = '%s'}", strings.Join(fields, ", "))
	ret += fmt.Sprintf(" -> %s", p.ChildPlan.String())
	return ret
}
