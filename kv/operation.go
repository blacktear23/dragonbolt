package kv

const (
	PUT = 1
	DEL = 2
	CAS = 3

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
