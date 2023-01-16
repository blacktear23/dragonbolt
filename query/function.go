package query

import (
	"fmt"
	"strconv"
	"strings"
)

var (
	funcMap = map[string]*Function{
		"lower": &Function{"lower", 1, false, funcToLower},
		"upper": &Function{"upper", 1, false, funcToUpper},
		"int":   &Function{"int", 1, false, funcToInt},
		"float": &Function{"float", 1, false, funcToFloat},
		"str":   &Function{"str", 1, false, funcToString},
	}
)

type FunctionBody func(kv KVPair, args []Expression) (any, error)

type Function struct {
	Name    string
	NumArgs int
	VarArgs bool
	Body    FunctionBody
}

func toString(value any) string {
	switch val := value.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32, float64:
		return fmt.Sprintf("%f", val)
	default:
		if val == nil {
			return "<nil>"
		}
		return ""
	}
}

func toInt(value any, defVal int64) int64 {
	switch val := value.(type) {
	case string:
		if ret, err := strconv.ParseInt(val, 10, 64); err == nil {
			return ret
		} else {
			return defVal
		}
	case []byte:
		if ret, err := strconv.ParseInt(string(val), 10, 64); err == nil {
			return ret
		} else {
			return defVal
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		if ret, err := strconv.ParseInt(fmt.Sprintf("%d", val), 10, 64); err == nil {
			return ret
		} else {
			return defVal
		}
	case float32:
		return int64(val)
	case float64:
		return int64(val)
	default:
		return defVal
	}
}

func toFloat(value any, defVal float64) float64 {
	switch val := value.(type) {
	case string:
		if ret, err := strconv.ParseFloat(val, 64); err == nil {
			return ret
		} else {
			return defVal
		}
	case []byte:
		if ret, err := strconv.ParseFloat(string(val), 64); err == nil {
			return ret
		} else {
			return defVal
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		if ret, err := strconv.ParseFloat(fmt.Sprintf("%d", val), 64); err == nil {
			return ret
		} else {
			return defVal
		}
	case float32:
		return float64(val)
	case float64:
		return val
	default:
		return defVal
	}
}

func funcToLower(kv KVPair, args []Expression) (any, error) {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return nil, err
	}
	arg := toString(rarg)
	return strings.ToLower(arg), nil
}

func funcToUpper(kv KVPair, args []Expression) (any, error) {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return nil, err
	}
	arg := toString(rarg)
	return strings.ToUpper(arg), nil
}

func funcToInt(kv KVPair, args []Expression) (any, error) {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return nil, err
	}
	ret := toInt(rarg, 0)
	return ret, nil
}

func funcToFloat(kv KVPair, args []Expression) (any, error) {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return nil, err
	}
	ret := toFloat(rarg, 0.0)
	return ret, nil
}

func funcToString(kv KVPair, args []Expression) (any, error) {
	rarg, err := args[0].Execute(kv)
	if err != nil {
		return nil, err
	}
	ret := toString(rarg)
	return ret, nil
}
