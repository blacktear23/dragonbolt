package txn

import "encoding/binary"

var (
	COL_LOCK  = []byte("l_")
	COL_WRITE = []byte("w_")
	COL_DATA  = []byte("d_")

	LOCK_PRIMARY   = byte('P')
	LOCK_SECONDARY = byte('S')
)

const UINT64_MAX uint64 = 0xFFFFFFFFFFFFFFFF

type KVPair struct {
	Key   []byte
	Value []byte
}

type KVOperation interface {
	GetTSO() (uint64, error)
	Set(kvs []KVPair) error
	Get(key []byte) ([]byte, error)
	Scan(start []byte, end []byte, limit int) ([][]byte, error)
}

func encodeKey(column []byte, key []byte, ts uint64) []byte {
	tsBytes := binary.BigEndian.Uint64(ts)
	ret := make([]byte, 0, len(key)+10)
	ret = append(ret, column...)
	ret = append(ret, key...)
	ret = append(ret, ':')
	ret = append(ret, tsBytes...)
	return ret
}

type Txn struct {
	kvOps   KVOperation
	startTs uint64
	primary []byte
}

func NewTxn(kvOps KVOperation) *Txn {
	return &Txn{
		kvOps: kvOps,
	}
}

func (t *Txn) Begin() error {
	tso, err := t.kvOps.GetTSO()
	if err != nil {
		return err
	}
	t.startTs = tso
	return nil
}

func (t *Txn) Set(key []byte, value []byte) error {
	return nil
}

func (t *Txn) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (t *Txn) Commit() error {
	return nil
}

func (t *Txn) prewrite(key []byte, val []byte) error {
	isPrimary := t.primary == nil

	if isPrimary {

		t.primary = key
	} else {

	}
	return nil
}

func (t *Txn) writeLock(key []byte, primary []byte) ([]byte, error) {
	lkey := encodeKey(COL_LOCK, key, t.startTs)
	dkey := encodeKey(COL_DATA, key, t.startTs)
	var lval []byte
	if primary == nil {
		lval = []byte{LOCK_PRIMARY}
	} else {
		lval = make([]byte, 1, len(primary)+1)
		lval[0] = LOCK_SECONDARY
		lval = append(lval, primary...)
	}
	kvs := []KVPair{
		KVPair{
			Key:   lkey,
			Value: lval,
		},
		KVPair{
			Key:   dkey,
			Value: dval,
		},
	}
	t.Set(kvs)
}
