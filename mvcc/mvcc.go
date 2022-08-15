package mvcc

import (
	"encoding/binary"
	"errors"
)

var (
	ErrKeyNotFound = errors.New("Key Not Found")
	ErrKeyLocked   = errors.New("Key Is Locked")
	ErrTxnConflict = errors.New("Transaction Conflict")
)

func IsKeyNotFoundError(err error) bool {
	return err == ErrKeyNotFound
}

func IsKeyLockedError(err error) bool {
	return err == ErrKeyLocked
}

func IsTxnConflictError(err error) bool {
	return err == ErrTxnConflict
}

type MvccDB interface {
	Get(ver uint64, key []byte) (val []byte, err error)
	Set(ver uint64, commitVer uint64, key []byte, val []byte) error
	Delete(ver uint64, commitVer uint64, key []byte) error
	Cursor(ver uint64) MvccCursor
	LockKey(ver uint64, key []byte) error
	UnlockKey(ver uint64, key []byte, force bool) error
}

type MvccCursor interface {
	Seek(start []byte) (key []byte, val []byte)
	Next() (key []byte, val []byte)
}

func encodeMvccKey(ver uint64, key []byte) []byte {
	klen := len(key)
	ret := make([]byte, klen+8)
	copy(ret, key)
	binary.BigEndian.PutUint64(ret[klen:], MAX_UINT64-ver)
	return ret
}

func decodeMvccKeyVersion(key []byte) uint64 {
	klen := len(key)
	if klen <= 8 {
		return 0
	}
	return MAX_UINT64 - binary.BigEndian.Uint64(key[klen-8:])
}

func decodeMvccKey(key []byte) ([]byte, uint64) {
	klen := len(key)
	if klen <= 8 {
		return nil, 0
	}
	rkey := key[0 : klen-8]
	dver := binary.BigEndian.Uint64(key[klen-8:])
	return rkey, MAX_UINT64 - dver
}

func encodeMvccValue(op byte, value []byte) []byte {
	ret := make([]byte, len(value)+1)
	ret[0] = op
	copy(ret[1:], value)
	return ret
}

func decodeMvccValue(value []byte) (byte, []byte) {
	if len(value) < 1 {
		return 0, nil
	}
	return value[0], value[1:]
}

func encodeLockValue(ver uint64) []byte {
	ret := make([]byte, 9)
	ret[0] = KEY_LOCK
	binary.BigEndian.PutUint64(ret[1:], ver)
	return ret
}
