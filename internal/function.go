package internal

import (
	"fmt"
	"regexp"
	"strings"
)

func ADD(a, b Value) (Value, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	return a.Add(b)
}

func SUB(a, b Value) (Value, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	return a.Sub(b)
}

func MUL(a, b Value) (Value, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	return a.Mul(b)
}

func OP_DIV(a, b Value) (Value, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	return a.Div(b)
}

func EQ(a, b Value) (Value, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	cond, err := a.EQ(b)
	if err != nil {
		return nil, err
	}
	return ValueOf(cond)
}

func NOT_EQ(a, b Value) (Value, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	cond, err := a.EQ(b)
	if err != nil {
		return nil, err
	}
	return ValueOf(!cond)
}

func GT(a, b Value) (Value, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	cond, err := a.GT(b)
	if err != nil {
		return nil, err
	}
	return ValueOf(cond)
}

func GTE(a, b Value) (Value, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	cond, err := a.GTE(b)
	if err != nil {
		return nil, err
	}
	return ValueOf(cond)
}

func LT(a, b Value) (Value, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	cond, err := a.LT(b)
	if err != nil {
		return nil, err
	}
	return ValueOf(cond)
}

func LTE(a, b Value) (Value, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	cond, err := a.LTE(b)
	if err != nil {
		return nil, err
	}
	return ValueOf(cond)
}

func BIT_NOT(a Value) (Value, error) {
	if a == nil {
		return nil, nil
	}
	v, err := a.ToInt64()
	if err != nil {
		return nil, err
	}
	return ValueOf(^v)
}

func BIT_LEFT_SHIFT(a, b Value) (Value, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	va, err := a.ToInt64()
	if err != nil {
		return nil, err
	}
	vb, err := b.ToInt64()
	if err != nil {
		return nil, err
	}
	return ValueOf(va << vb)
}

func BIT_RIGHT_SHIFT(a, b Value) (Value, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	va, err := a.ToInt64()
	if err != nil {
		return nil, err
	}
	vb, err := b.ToInt64()
	if err != nil {
		return nil, err
	}
	return ValueOf(va >> vb)
}

func BIT_AND(a, b Value) (Value, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	va, err := a.ToInt64()
	if err != nil {
		return nil, err
	}
	vb, err := b.ToInt64()
	if err != nil {
		return nil, err
	}
	return ValueOf(va & vb)
}

func BIT_OR(a, b Value) (Value, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	va, err := a.ToInt64()
	if err != nil {
		return nil, err
	}
	vb, err := b.ToInt64()
	if err != nil {
		return nil, err
	}
	return ValueOf(va | vb)
}

func BIT_XOR(a, b Value) (Value, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	va, err := a.ToInt64()
	if err != nil {
		return nil, err
	}
	vb, err := b.ToInt64()
	if err != nil {
		return nil, err
	}
	return ValueOf(va ^ vb)
}

func ARRAY_IN(a, b Value) (Value, error) {
	array, err := b.ToArray()
	if err != nil {
		return nil, err
	}
	cond, err := array.Has(a)
	if err != nil {
		return nil, err
	}
	return ValueOf(cond)
}

func STRUCT_FIELD(v Value, idx int) (Value, error) {
	sv, err := v.ToStruct()
	if err != nil {
		return nil, err
	}
	return sv.values[idx], nil
}

func ARRAY_OFFSET(v Value, idx int) (Value, error) {
	array, err := v.ToArray()
	if err != nil {
		return nil, err
	}
	if idx < 0 || len(array.values) <= idx {
		return nil, fmt.Errorf("OFFSET(%d) is out of range", idx)
	}
	return array.values[idx], nil
}

func ARRAY_SAFE_OFFSET(v Value, idx int) (Value, error) {
	array, err := v.ToArray()
	if err != nil {
		return nil, err
	}
	if idx < 0 || len(array.values) <= idx {
		return nil, nil
	}
	return array.values[idx], nil
}

func ARRAY_ORDINAL(v Value, idx int) (Value, error) {
	array, err := v.ToArray()
	if err != nil {
		return nil, err
	}
	if idx < 1 || len(array.values) <= idx {
		return nil, fmt.Errorf("ORDINAL(%d) is out of range", idx)
	}
	return array.values[idx-1], nil
}

func ARRAY_SAFE_ORDINAL(v Value, idx int) (Value, error) {
	array, err := v.ToArray()
	if err != nil {
		return nil, err
	}
	if idx < 1 || len(array.values) <= idx {
		return nil, nil
	}
	return array.values[idx-1], nil
}

func CONCAT(a, b Value) (Value, error) {
	return a.Add(b)
}

func LIKE(a, b Value) (Value, error) {
	va, err := a.ToString()
	if err != nil {
		return nil, err
	}
	vb, err := b.ToString()
	if err != nil {
		return nil, err
	}
	re, err := regexp.Compile(strings.Replace(vb, "%", "*", -1))
	if err != nil {
		return nil, err
	}
	return ValueOf(re.MatchString(va))
}

func BETWEEN(target, start, end Value) (Value, error) {
	t, err := target.ToInt64()
	if err != nil {
		return nil, err
	}
	s, err := start.ToInt64()
	if err != nil {
		return nil, err
	}
	e, err := end.ToInt64()
	if err != nil {
		return nil, err
	}
	if s <= t && t <= e {
		return BoolValue(true), nil
	}
	return BoolValue(false), nil
}

func IN(a Value, values ...Value) (Value, error) {
	for _, v := range values {
		cond, err := a.EQ(v)
		if err != nil {
			return nil, err
		}
		if cond {
			return ValueOf(true)
		}
	}
	return ValueOf(false)
}

func IS_NULL(a Value) (Value, error) {
	return ValueOf(a == nil)
}

func IS_TRUE(a Value) (Value, error) {
	if a == nil {
		return nil, nil
	}
	b, err := a.ToBool()
	if err != nil {
		return nil, err
	}
	return ValueOf(b)
}

func IS_FALSE(a Value) (Value, error) {
	if a == nil {
		return nil, nil
	}
	b, err := a.ToBool()
	if err != nil {
		return nil, err
	}
	return ValueOf(!b)
}

func NOT(a Value) (Value, error) {
	if a == nil {
		return nil, nil
	}
	v, err := a.ToInt64()
	if err != nil {
		return nil, err
	}
	return BoolValue(v == 0), nil
}

func AND(args ...Value) (Value, error) {
	for _, v := range args {
		if v == nil {
			return nil, nil
		}
		cond, err := v.ToBool()
		if err != nil {
			return nil, err
		}
		if !cond {
			return BoolValue(false), nil
		}
	}
	return BoolValue(true), nil
}

func OR(args ...Value) (Value, error) {
	for _, v := range args {
		if v == nil {
			return nil, nil
		}
		cond, err := v.ToBool()
		if err != nil {
			return nil, err
		}
		if cond {
			return BoolValue(true), nil
		}
	}
	return BoolValue(false), nil
}

func CASE_WITH_VALUE(caseV Value, args ...Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("when value must be specified")
	}
	for i := 0; i < len(args)-1; i += 2 {
		when := args[i]
		then := args[i+1]
		cond, err := caseV.EQ(when)
		if err != nil {
			return nil, err
		}
		if cond {
			return then, nil
		}
	}
	// if args length is odd number, else statement exists.
	if len(args) > (len(args)/2)*2 {
		return args[len(args)-1], nil
	}
	// if else statment not exists, returns NULL.
	return nil, nil
}

func CASE_NO_VALUE(args ...Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("when value must be specified")
	}
	for i := 0; i < len(args)-1; i += 2 {
		when := args[i]
		then := args[i+1]
		if when == nil {
			continue
		}
		cond, err := when.ToBool()
		if err != nil {
			return nil, err
		}
		if cond {
			return then, nil
		}
	}
	// if args length is odd number, else statement exists.
	if len(args) > (len(args)/2)*2 {
		return args[len(args)-1], nil
	}
	// if else statment not exists, returns NULL.
	return nil, nil
}

func COALESCE(args ...Value) (Value, error) {
	for _, arg := range args {
		if arg == nil {
			continue
		}
		return arg, nil
	}
	return nil, fmt.Errorf("COALESCE requried arguments")
}

func IF(cond, trueV, falseV Value) (Value, error) {
	if cond == nil {
		return falseV, nil
	}
	b, err := cond.ToBool()
	if err != nil {
		return nil, err
	}
	if b {
		return trueV, nil
	}
	return falseV, nil
}

func IFNULL(expr, nullResult Value) (Value, error) {
	if expr == nil {
		return nullResult, nil
	}
	return expr, nil
}

func NULLIF(expr, exprToMatch Value) (Value, error) {
	cond, err := expr.EQ(exprToMatch)
	if err != nil {
		return nil, err
	}
	if cond {
		return nil, nil
	}
	return expr, nil
}

func LENGTH(v Value) (Value, error) {
	if v == nil {
		return IntValue(0), nil
	}
	s, err := v.ToString()
	if err != nil {
		return nil, err
	}
	return IntValue(int64(len(s))), nil
}

func DECODE_ARRAY(v string) (Value, error) {
	json, err := jsonArrayFromEncodedString(v)
	if err != nil {
		return nil, err
	}
	return StringValue(json), nil
}

func MAKE_STRUCT(args ...Value) (Value, error) {
	keys := make([]string, len(args))
	fieldMap := map[string]Value{}
	for i := 0; i < len(args); i++ {
		key := fmt.Sprintf("_field_%d", i+1)
		keys[i] = key
		fieldMap[key] = args[i]
	}
	return &StructValue{
		keys:   keys,
		values: args,
		m:      fieldMap,
	}, nil
}
