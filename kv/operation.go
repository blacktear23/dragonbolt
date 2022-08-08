package kv

const (
	PUT = 1
	DEL = 2
	CAS = 3

	GET        = 4
	GET_VALUE  = 5
	SCAN       = 6
	SCAN_KEY   = 7
	SCAN_VALUE = 8

	RESULT_ERR  uint64 = 0
	RESULT_OK   uint64 = 1
	RESULT_FAIL uint64 = 2
)

type Mutation struct {
	Op        int      `json:"op"`
	Keys      [][]byte `json:"keys"`
	Values    [][]byte `json:"vals"`
	NewValues [][]byte `json:"nvals"`
}

type Query struct {
	Op    int `json:"op"`
	Key   []byte
	Start []byte
	End   []byte
	Limit int
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
	case SCAN_KEY:
		return KVPair{
			Key: key,
		}
	case SCAN_VALUE, GET_VALUE:
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
