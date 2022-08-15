package kv

import "github.com/blacktear23/dragonbolt/mvcc"

const (
	// Normal Mutation Operations
	PUT = 1
	DEL = 2
	CAS = 3

	// Column Family Related Mutation Operations
	CF_PUT = 4
	CF_DEL = 5

	// Normal Query Operations
	GET        = 4
	GET_VALUE  = 5
	SCAN       = 6
	SCAN_KEY   = 7
	SCAN_VALUE = 8

	// Column Family Related Query Operations
	CF_GET         = 9
	CF_GET_VALUE   = 10
	CF_SCAN        = 11
	CF_SCAN_KEY    = 12
	CF_SCAN_VALUE  = 13
	CF_RSCAN       = 14
	CF_RSCAN_KEY   = 15
	CF_RSCAN_VALUE = 16

	MVCC_GET          = 20
	MVCC_GET_VALUE    = 21
	MVCC_SET          = 22
	MVCC_DEL          = 23
	MVCC_SCAN         = 24
	MVCC_SCAN_KEY     = 25
	MVCC_SCAN_VALUE   = 26
	MVCC_LOCK         = 27
	MVCC_UNLOCK       = 28
	MVCC_UNLOCK_FORCE = 29
	MVCC_GC           = 30

	RESULT_ERR          uint64 = 0
	RESULT_OK           uint64 = 1
	RESULT_FAIL         uint64 = 2
	RESULT_KEY_LOCKED   uint64 = 3
	RESULT_TXN_CONFLICT uint64 = 4

	CFData  = "cf:data"
	CFLock  = "cf:lock"
	CFWrite = "cf:write"
)

type Mutation struct {
	Op            int    `json:"op"`
	Cf            string `json:"cf"`
	Key           []byte `json:"key"`
	Value         []byte `json:"val"`
	NewValue      []byte `json:"nval"`
	Version       uint64 `json:"ver"`
	CommitVersion uint64 `json:"commit_ver"`
}

type Query struct {
	Op      int      `json:"op"`
	Cf      string   `json:"cf"`
	Keys    [][]byte `json:"keys"`
	Start   []byte   `json:"start"`
	End     []byte   `json:"end"`
	Limit   int      `json:"limit"`
	SameLen bool     `json:"samelen"`
	Version uint64   `json:"ver"`
}

type KVPair struct {
	Key   []byte
	Value []byte
}

type QueryResult struct {
	KVS []KVPair
}

func buildKVP(key, value []byte, op int) KVPair {
	switch op {
	case SCAN_KEY, CF_SCAN_KEY, CF_RSCAN_KEY, MVCC_SCAN_KEY:
		return KVPair{
			Key: key,
		}
	case SCAN_VALUE, GET_VALUE, CF_GET_VALUE, CF_SCAN_VALUE, CF_RSCAN_VALUE, MVCC_GET_VALUE, MVCC_SCAN_VALUE:
		return KVPair{
			Value: value,
		}
	default:
		return KVPair{
			Key:   key,
			Value: value,
		}
	}
}

func (qr *QueryResult) AddKVPair(key, value []byte, op int) {
	qr.KVS = append(qr.KVS, buildKVP(key, value, op))
}

func CheckColumnFamily(cf string) error {
	switch cf {
	case CFData, CFLock, CFWrite, mvcc.CFKeys, mvcc.CFValues:
		return nil
	default:
		return ErrInvalidColumnFamily
	}
}
