package query

import (
	"bytes"
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
