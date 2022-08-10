package txn

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/blacktear23/dragonbolt/kv"
)

var (
	LOCK_PRIMARY   = byte('P')
	LOCK_SECONDARY = byte('S')

	ErrWaitLockTimeout = errors.New("Wait Lock Timeout")
)

func encodeKey(column []byte, key []byte, ts uint64) []byte {
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, ts)
	ret := make([]byte, 0, len(key)+10)
	ret = append(ret, column...)
	ret = append(ret, key...)
	ret = append(ret, ':')
	ret = append(ret, tsBytes...)
	return ret
}

type MvccTxn struct {
	ops     KVCfOperation
	startTs uint64
	memdb   *MemDB
	primary []byte
}

func NewTxn(kvOps KVCfOperation) *MvccTxn {
	return &MvccTxn{
		ops:   kvOps,
		memdb: NewMemDB(),
	}
}

func (t *MvccTxn) Begin() error {
	tso, err := t.ops.GetTSO()
	if err != nil {
		return err
	}
	t.startTs = tso
	return nil
}

func (t *MvccTxn) Set(key []byte, value []byte) error {
	return t.memdb.Set(key, value)
}

func (t *MvccTxn) Get(key []byte) ([]byte, error) {
	value, have := t.memdb.Get(key)
	// have and value is nil means delete it
	if have {
		return value, nil
	}
	return t.tsGet(key)
}

func (t *MvccTxn) tsGet(key []byte) ([]byte, error) {
	keyZeroTs := encodeTsKey(key, 0)
	keyStartTs := encodeTsKey(key, t.startTs)
	noLock := false
	for i := 0; i < 100; i++ {
		kvs, err := t.ops.Scan(kv.CFLock, keyZeroTs, keyStartTs, 10)
		if err != nil {
			return nil, err
		}
		if len(kvs) == 0 {
			noLock = true
			break
		}
		sdur := min(1<<i, 500)
		time.Sleep(time.Duration(sdur) * time.Millisecond)
	}
	if !noLock {
		return nil, ErrWaitLockTimeout
	}
	// lastWrite, err := t.ops.Scan(kv.CFWrite, keyZeroTs, keyStartTs, 10)
	return nil, nil
}

func (t *MvccTxn) Delete(key []byte) error {
	return t.memdb.Delete(key)
}

func (t *MvccTxn) Commit() error {
	return nil
}

func (t *MvccTxn) prewrite(key []byte, val []byte) error {
	isPrimary := t.primary == nil

	if isPrimary {

		t.primary = key
	} else {

	}
	return nil
}

func encodeTsKey(key []byte, ts uint64) []byte {
	ret := make([]byte, len(key)+8)
	copy(ret[0:], key)
	binary.BigEndian.PutUint64(ret[len(key):], ts)
	return ret
}
