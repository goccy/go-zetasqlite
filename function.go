package zetasqlite

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/mattn/go-sqlite3"
)

const (
	notEqualFuncName                 = "zetasqlite_not_equal_bool"
	equalFuncName                    = "zetasqlite_equal_bool"
	greaterFuncName                  = "zetasqlite_greater_bool"
	greaterOrEqualFuncName           = "zetasqlite_greater_or_equal_bool"
	lessFuncName                     = "zetasqlite_less_bool"
	lessOrEqualFuncName              = "zetasqlite_less_or_equal_bool"
	inArrayFuncName                  = "zetasqlite_in_array_bool"
	addI64FuncName                   = "zetasqlite_add_int64"
	subI64FuncName                   = "zetasqlite_subtract_int64"
	mulI64FuncName                   = "zetasqlite_multiply_int64"
	divI64FuncName                   = "zetasqlite_div_int64"
	divDoubleFuncName                = "zetasqlite_divide_double"
	bitwiseNotI64FuncName            = "zetasqlite_bitwise_not_int64"
	bitwiseLeftShiftI64FuncName      = "zetasqlite_bitwise_left_shift_int64"
	bitwiseAndI64FuncName            = "zetasqlite_bitwise_and_int64"
	bitwiseOrI64FuncName             = "zetasqlite_bitwise_or_int64"
	bitwiseXorI64FuncName            = "zetasqlite_bitwise_xor_int64"
	bitwiseRightShiftI64FuncName     = "zetasqlite_bitwise_right_shift_int64"
	getStructFieldI64FuncName        = "zetasqlite_get_struct_field_int64"
	getStructFieldStringFuncName     = "zetasqlite_get_struct_field_string"
	getStructFieldStructFuncName     = "zetasqlite_get_struct_field_struct"
	sumI64FuncName                   = "zetasqlite_sum_int64"
	decodeArrayFuncName              = "zetasqlite_decode_array"
	concatStringFuncName             = "zetasqlite_concat_string"
	likeFuncName                     = "zetasqlite_like_bool"
	betweenFuncName                  = "zetasqlite_between_bool"
	inFuncName                       = "zetasqlite_in_bool"
	isNullFuncName                   = "zetasqlite_is_null_bool"
	isTrueFuncName                   = "zetasqlite_is_true_bool"
	isFalseFuncName                  = "zetasqlite_is_false_bool"
	notFuncName                      = "zetasqlite_not_bool"
	andFuncName                      = "zetasqlite_and_bool"
	orFuncName                       = "zetasqlite_or_bool"
	caseWithValueStringFuncName      = "zetasqlite_case_with_value_string"
	caseNoValueStringFuncName        = "zetasqlite_case_no_value_string"
	coalesceStringFuncName           = "zetasqlite_coalesce_string"
	ifI64FuncName                    = "zetasqlite_if_int64"
	ifStringFuncName                 = "zetasqlite_if_string"
	ifnullI64FuncName                = "zetasqlite_ifnull_int64"
	nullifI64FuncName                = "zetasqlite_nullif_int64"
	arrayAtOffsetStringFuncName      = "zetasqlite_array_at_offset_string"
	arrayAtOrdinalStringFuncName     = "zetasqlite_array_at_ordinal_string"
	safeArrayAtOffsetStringFuncName  = "zetasqlite_safe_array_at_offset_string"
	safeArrayAtOrdinalStringFuncName = "zetasqlite_safe_array_at_ordinal_string"
)

var (
	zetasqliteFuncMap = map[string]interface{}{
		notEqualFuncName:                 notEqualFunc,
		equalFuncName:                    equalFunc,
		greaterFuncName:                  greaterFunc,
		greaterOrEqualFuncName:           greaterOrEqualFunc,
		lessFuncName:                     lessFunc,
		lessOrEqualFuncName:              lessOrEqualFunc,
		inArrayFuncName:                  inArrayFunc,
		addI64FuncName:                   addI64Func,
		subI64FuncName:                   subI64Func,
		mulI64FuncName:                   mulI64Func,
		divI64FuncName:                   divI64Func,
		divDoubleFuncName:                divDoubleFunc,
		bitwiseNotI64FuncName:            bitwiseNotI64Func,
		bitwiseLeftShiftI64FuncName:      bitwiseLeftShiftI64Func,
		bitwiseRightShiftI64FuncName:     bitwiseRightShiftI64Func,
		bitwiseAndI64FuncName:            bitwiseAndI64Func,
		bitwiseOrI64FuncName:             bitwiseOrI64Func,
		bitwiseXorI64FuncName:            bitwiseXorI64Func,
		getStructFieldI64FuncName:        getStructFieldI64Func,
		getStructFieldStringFuncName:     getStructFieldStringFunc,
		getStructFieldStructFuncName:     getStructFieldStructFunc,
		decodeArrayFuncName:              decodeArrayFunc,
		concatStringFuncName:             concatStringFunc,
		likeFuncName:                     likeFunc,
		betweenFuncName:                  betweenFunc,
		inFuncName:                       inFunc,
		isNullFuncName:                   isNullFunc,
		isTrueFuncName:                   isTrueFunc,
		isFalseFuncName:                  isFalseFunc,
		notFuncName:                      notFunc,
		andFuncName:                      andFunc,
		orFuncName:                       orFunc,
		caseWithValueStringFuncName:      caseWithValueStringFunc,
		caseNoValueStringFuncName:        caseNoValueStringFunc,
		coalesceStringFuncName:           coalesceStringFunc,
		ifI64FuncName:                    ifI64Func,
		ifStringFuncName:                 ifStringFunc,
		ifnullI64FuncName:                ifnullI64Func,
		nullifI64FuncName:                nullifI64Func,
		arrayAtOffsetStringFuncName:      arrayAtOffsetStringFunc,
		arrayAtOrdinalStringFuncName:     arrayAtOrdinalStringFunc,
		safeArrayAtOffsetStringFuncName:  safeArrayAtOffsetStringFunc,
		safeArrayAtOrdinalStringFuncName: safeArrayAtOrdinalStringFunc,
	}
	zetasqliteAggregatorFuncMap = map[string]interface{}{
		sumI64FuncName: newSumFunc,
	}
	builtinFunctions = []string{
		"if", "concat", "coalesce", "ifnull", "nullif",
	}
	builtinAggregateFunctions = []string{
		"sum",
	}
	builtinFuncMap          = map[string]struct{}{}
	builtinAggregateFuncMap = map[string]struct{}{}
)

func init() {
	for _, f := range builtinFunctions {
		builtinFuncMap[f] = struct{}{}
	}
	for _, f := range builtinAggregateFunctions {
		builtinAggregateFuncMap[f] = struct{}{}
	}
}

func registerBuiltinFunctions(conn *sqlite3.SQLiteConn) error {
	for name, fn := range zetasqliteFuncMap {
		if err := conn.RegisterFunc(name, fn, true); err != nil {
			return fmt.Errorf("failed to register builtin function %s: %w", name, err)
		}
	}
	for name, agg := range zetasqliteAggregatorFuncMap {
		if err := conn.RegisterAggregator(name, agg, true); err != nil {
			return fmt.Errorf("failed to register aggregator function %s: %w", name, err)
		}
	}
	return nil
}

func newSumFunc() *sumFunc { return &sumFunc{} }

func addI64Func(a, b interface{}) (int64, error) {
	va, err := ValueOf(a)
	if err != nil {
		return 0, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return 0, err
	}
	ret, err := va.Add(vb)
	if err != nil {
		return 0, err
	}
	return ret.ToInt64()
}

func subI64Func(a, b interface{}) (int64, error) {
	va, err := ValueOf(a)
	if err != nil {
		return 0, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return 0, err
	}
	ret, err := va.Sub(vb)
	if err != nil {
		return 0, err
	}
	return ret.ToInt64()
}

func mulI64Func(a, b interface{}) (int64, error) {
	va, err := ValueOf(a)
	if err != nil {
		return 0, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return 0, err
	}
	ret, err := va.Mul(vb)
	if err != nil {
		return 0, err
	}
	return ret.ToInt64()
}

func divI64Func(a, b interface{}) (int64, error) {
	va, err := ValueOf(a)
	if err != nil {
		return 0, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return 0, err
	}
	ret, err := va.Div(vb)
	if err != nil {
		return 0, err
	}
	return ret.ToInt64()
}

func divDoubleFunc(a, b interface{}) (float64, error) {
	va, err := ValueOf(a)
	if err != nil {
		return 0, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return 0, err
	}
	ret, err := va.Div(vb)
	if err != nil {
		return 0, err
	}
	return ret.ToFloat64()
}

func bitwiseNotI64Func(a interface{}) (int64, error) {
	va, err := ValueOf(a)
	if err != nil {
		return 0, err
	}
	i64, err := va.ToInt64()
	if err != nil {
		return 0, err
	}
	return ^i64, nil
}

func bitwiseLeftShiftI64Func(a, b interface{}) (int64, error) {
	va, err := ValueOf(a)
	if err != nil {
		return 0, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return 0, err
	}
	ia, err := va.ToInt64()
	if err != nil {
		return 0, err
	}
	ib, err := vb.ToInt64()
	if err != nil {
		return 0, err
	}
	return ia << ib, nil
}

func bitwiseRightShiftI64Func(a, b interface{}) (int64, error) {
	va, err := ValueOf(a)
	if err != nil {
		return 0, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return 0, err
	}
	ia, err := va.ToInt64()
	if err != nil {
		return 0, err
	}
	ib, err := vb.ToInt64()
	if err != nil {
		return 0, err
	}
	return ia >> ib, nil
}

func bitwiseAndI64Func(a, b interface{}) (int64, error) {
	va, err := ValueOf(a)
	if err != nil {
		return 0, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return 0, err
	}
	ia, err := va.ToInt64()
	if err != nil {
		return 0, err
	}
	ib, err := vb.ToInt64()
	if err != nil {
		return 0, err
	}
	return ia & ib, nil
}

func bitwiseOrI64Func(a, b interface{}) (int64, error) {
	va, err := ValueOf(a)
	if err != nil {
		return 0, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return 0, err
	}
	ia, err := va.ToInt64()
	if err != nil {
		return 0, err
	}
	ib, err := vb.ToInt64()
	if err != nil {
		return 0, err
	}
	return ia | ib, nil
}

func bitwiseXorI64Func(a, b interface{}) (int64, error) {
	va, err := ValueOf(a)
	if err != nil {
		return 0, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return 0, err
	}
	ia, err := va.ToInt64()
	if err != nil {
		return 0, err
	}
	ib, err := vb.ToInt64()
	if err != nil {
		return 0, err
	}
	return ia ^ ib, nil
}

func notEqualFunc(a, b interface{}) (bool, error) {
	va, err := ValueOf(a)
	if err != nil {
		return false, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return false, err
	}
	cond, err := va.EQ(vb)
	if err != nil {
		return false, err
	}
	return !cond, nil
}

func equalFunc(a, b interface{}) (bool, error) {
	va, err := ValueOf(a)
	if err != nil {
		return false, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return false, err
	}
	return va.EQ(vb)
}

func greaterFunc(a, b interface{}) (bool, error) {
	va, err := ValueOf(a)
	if err != nil {
		return false, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return false, err
	}
	return va.GT(vb)
}

func greaterOrEqualFunc(a, b interface{}) (bool, error) {
	va, err := ValueOf(a)
	if err != nil {
		return false, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return false, err
	}
	return va.GTE(vb)
}

func lessFunc(a, b interface{}) (bool, error) {
	va, err := ValueOf(a)
	if err != nil {
		return false, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return false, err
	}
	return va.LT(vb)
}

func lessOrEqualFunc(a, b interface{}) (bool, error) {
	va, err := ValueOf(a)
	if err != nil {
		return false, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return false, err
	}
	return va.LTE(vb)
}

func inArrayFunc(a, b interface{}) (bool, error) {
	va, err := ValueOf(a)
	if err != nil {
		return false, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return false, err
	}
	array, err := vb.ToArray()
	if err != nil {
		return false, err
	}
	return array.Has(va)
}

func getStructFieldI64Func(v interface{}, fieldIdx int) (int64, error) {
	vv, err := ValueOf(v)
	if err != nil {
		return 0, err
	}
	sv, err := vv.ToStruct()
	if err != nil {
		return 0, err
	}
	key := sv.keys[fieldIdx]
	return sv.m[key].ToInt64()
}

func getStructFieldStringFunc(v interface{}, fieldIdx int) (string, error) {
	vv, err := ValueOf(v)
	if err != nil {
		return "", err
	}
	sv, err := vv.ToStruct()
	if err != nil {
		return "", err
	}
	key := sv.keys[fieldIdx]
	return sv.m[key].ToString()
}

func getStructFieldStructFunc(v interface{}, fieldIdx int) (string, error) {
	vv, err := ValueOf(v)
	if err != nil {
		return "", err
	}
	sv, err := vv.ToStruct()
	if err != nil {
		return "", err
	}
	key := sv.keys[fieldIdx]
	return sv.m[key].ToString()
}

func arrayAtOffsetStringFunc(v interface{}, arrayIdx int) (string, error) {
	vv, err := ValueOf(v)
	if err != nil {
		return "", err
	}
	av, err := vv.ToArray()
	if err != nil {
		return "", err
	}
	if arrayIdx < 0 || len(av.values) <= arrayIdx {
		return "", fmt.Errorf("OFFSET(%d) is out of range", arrayIdx)
	}
	elem := av.values[arrayIdx]
	return elem.ToString()
}

func arrayAtOrdinalStringFunc(v interface{}, arrayIdx int) (string, error) {
	vv, err := ValueOf(v)
	if err != nil {
		return "", err
	}
	av, err := vv.ToArray()
	if err != nil {
		return "", err
	}
	if arrayIdx < 1 || len(av.values) <= arrayIdx {
		return "", fmt.Errorf("ORDINAL(%d) is out of range", arrayIdx)
	}
	elem := av.values[arrayIdx-1]
	return elem.ToString()
}

func safeArrayAtOffsetStringFunc(v interface{}, arrayIdx int) (string, error) {
	vv, err := ValueOf(v)
	if err != nil {
		return "", err
	}
	av, err := vv.ToArray()
	if err != nil {
		return "", err
	}
	if arrayIdx < 0 || len(av.values) <= arrayIdx {
		return "NULL", nil
	}
	elem := av.values[arrayIdx]
	return elem.ToString()
}

func safeArrayAtOrdinalStringFunc(v interface{}, arrayIdx int) (string, error) {
	vv, err := ValueOf(v)
	if err != nil {
		return "", err
	}
	av, err := vv.ToArray()
	if err != nil {
		return "", err
	}
	if arrayIdx < 1 || len(av.values) <= arrayIdx {
		return "NULL", nil
	}
	elem := av.values[arrayIdx-1]
	return elem.ToString()
}

func concatStringFunc(a, b interface{}) (string, error) {
	va, err := ValueOf(a)
	if err != nil {
		return "", err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return "", err
	}
	sa, err := va.ToString()
	if err != nil {
		return "", err
	}
	sb, err := vb.ToString()
	if err != nil {
		return "", err
	}
	return sa + sb, nil
}

func likeFunc(a, b interface{}) (bool, error) {
	va, err := ValueOf(a)
	if err != nil {
		return false, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return false, err
	}
	sa, err := va.ToString()
	if err != nil {
		return false, err
	}
	sb, err := vb.ToString()
	if err != nil {
		return false, err
	}
	re, err := regexp.Compile(strings.Replace(sb, "%", "*", -1))
	if err != nil {
		return false, err
	}
	return re.MatchString(sa), nil
}

func betweenFunc(target, start, end string) (bool, error) {
	targetTime, err := toTimeValue(target)
	if err != nil {
		return false, err
	}
	startTime, err := toTimeValue(start)
	if err != nil {
		return false, err
	}
	endTime, err := toTimeValue(end)
	if err != nil {
		return false, err
	}
	return targetTime.After(startTime) && targetTime.Before(endTime), nil
}

func inFunc(a interface{}, values ...interface{}) (bool, error) {
	va, err := ValueOf(a)
	if err != nil {
		return false, err
	}
	for _, v := range values {
		vv, err := ValueOf(v)
		if err != nil {
			return false, err
		}
		cond, err := va.EQ(vv)
		if err != nil {
			return false, err
		}
		if cond {
			return true, nil
		}
	}
	return false, nil
}

func isNullFunc(a interface{}) (bool, error) {
	if _, ok := a.([]byte); !ok {
		return false, nil
	}
	va, err := ValueOf(a)
	if err != nil {
		return false, err
	}
	s, err := va.ToString()
	if err != nil {
		return false, err
	}
	return s == "", nil
}

func isTrueFunc(a interface{}) (bool, error) {
	va, err := ValueOf(a)
	if err != nil {
		return false, err
	}
	b, err := va.ToBool()
	if err != nil {
		return false, err
	}
	return b, nil
}

func isFalseFunc(a interface{}) (bool, error) {
	va, err := ValueOf(a)
	if err != nil {
		return false, err
	}
	b, err := va.ToBool()
	if err != nil {
		return false, err
	}
	return !b, nil
}

func notFunc(a interface{}) (bool, error) {
	va, err := ValueOf(a)
	if err != nil {
		return false, err
	}
	i, err := va.ToInt64()
	if err != nil {
		return false, err
	}
	return i == 0, nil
}

func andFunc(a, b interface{}) (bool, error) {
	va, err := ValueOf(a)
	if err != nil {
		return false, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return false, err
	}
	ba, err := va.ToBool()
	if err != nil {
		return false, err
	}
	bb, err := vb.ToBool()
	if err != nil {
		return false, err
	}
	return ba && bb, nil
}

func orFunc(a, b interface{}) (bool, error) {
	va, err := ValueOf(a)
	if err != nil {
		return false, err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return false, err
	}
	ba, err := va.ToBool()
	if err != nil {
		return false, err
	}
	bb, err := vb.ToBool()
	if err != nil {
		return false, err
	}
	return ba || bb, nil
}

func caseWithValueStringFunc(v interface{}, args ...interface{}) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("when value must be specified")
	}
	caseV, err := ValueOf(v)
	if err != nil {
		return "", err
	}
	for i := 0; i < len(args)-1; i += 2 {
		when := args[i]
		then := args[i+1]
		whenV, err := ValueOf(when)
		if err != nil {
			return "", err
		}
		thenV, err := ValueOf(then)
		if err != nil {
			return "", err
		}
		cond, err := caseV.EQ(whenV)
		if err != nil {
			return "", err
		}
		if cond {
			return thenV.ToString()
		}
	}
	// if args length is odd number, else statement exists.
	if len(args) > (len(args)/2)*2 {
		lastV, err := ValueOf(args[len(args)-1])
		if err != nil {
			return "", err
		}
		return lastV.ToString()
	}
	// if else statment not exists, returns NULL.
	return "NULL", nil
}

func caseNoValueStringFunc(args ...interface{}) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("when value must be specified")
	}
	for i := 0; i < len(args)-1; i += 2 {
		when := args[i]
		then := args[i+1]
		whenV, err := ValueOf(when)
		if err != nil {
			return "", err
		}
		thenV, err := ValueOf(then)
		if err != nil {
			return "", err
		}
		cond, err := whenV.ToBool()
		if err != nil {
			return "", err
		}
		if cond {
			return thenV.ToString()
		}
	}
	// if args length is odd number, else statement exists.
	if len(args) > (len(args)/2)*2 {
		lastV, err := ValueOf(args[len(args)-1])
		if err != nil {
			return "", err
		}
		return lastV.ToString()
	}
	// if else statment not exists, returns NULL.
	return "NULL", nil
}

func coalesceStringFunc(args ...interface{}) (string, error) {
	for _, arg := range args {
		if v, ok := arg.([]byte); ok {
			if len(v) == 0 {
				continue
			}
		}
		v, err := ValueOf(arg)
		if err != nil {
			return "", err
		}
		return v.ToString()
	}
	return "", fmt.Errorf("COALESCE requried arguments")
}

func ifI64Func(cond bool, trueV interface{}, falseV interface{}) (int64, error) {
	if cond {
		v, err := ValueOf(trueV)
		if err != nil {
			return 0, err
		}
		return v.ToInt64()
	}
	v, err := ValueOf(falseV)
	if err != nil {
		return 0, err
	}
	return v.ToInt64()
}

func ifStringFunc(cond bool, trueV interface{}, falseV interface{}) (string, error) {
	if cond {
		v, err := ValueOf(trueV)
		if err != nil {
			return "", err
		}
		return v.ToString()
	}
	v, err := ValueOf(falseV)
	if err != nil {
		return "", err
	}
	return v.ToString()
}

func ifnullI64Func(expr, nullResult interface{}) (int64, error) {
	if isNULLValue(expr) {
		v, err := ValueOf(nullResult)
		if err != nil {
			return 0, err
		}
		return v.ToInt64()
	}
	v, err := ValueOf(expr)
	if err != nil {
		return 0, err
	}
	return v.ToInt64()
}

func nullifI64Func(expr, exprToMatch interface{}) (int64, error) {
	exprV, err := ValueOf(expr)
	if err != nil {
		return 0, err
	}
	exprToMatchV, err := ValueOf(exprToMatch)
	if err != nil {
		return 0, err
	}
	cond, err := exprV.EQ(exprToMatchV)
	if err != nil {
		return 0, err
	}
	if cond {
		return 0, nil
	}
	return exprV.ToInt64()
}

type sumFunc struct {
	sum int64
}

func (s *sumFunc) Step(v int64) {
	s.sum += v
}

func (s *sumFunc) Done() int64 {
	return s.sum
}

func decodeArrayFunc(v string) (string, error) {
	json, err := jsonArrayFromEncodedString(v)
	if err != nil {
		return "", err
	}
	return string(json), nil
}
