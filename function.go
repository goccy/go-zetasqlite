package zetasqlite

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

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
	addDateFuncName                  = "zetasqlite_add_date"
	subI64FuncName                   = "zetasqlite_subtract_int64"
	subDateFuncName                  = "zetasqlite_subtract_date"
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
	bitAndFuncName                   = "zetasqlite_bit_and_int64"
	bitOrFuncName                    = "zetasqlite_bit_or_int64"
	bitXorFuncName                   = "zetasqlite_bit_xor_int64"
	countI64FuncName                 = "zetasqlite_count_int64"
	countStarI64FuncName             = "zetasqlite_count_star_int64"
	countifI64FuncName               = "zetasqlite_countif_int64"
	logicalAndFuncName               = "zetasqlite_logical_and_bool"
	logicalOrFuncName                = "zetasqlite_logical_or_bool"
	stringAggFuncName                = "zetasqlite_string_agg_string"
	distinctOptFuncName              = "zetasqlite_distinct_opt"
	limitOptFuncName                 = "zetasqlite_limit_opt"
	orderByOptI64FuncName            = "zetasqlite_order_by_opt_int64"
	orderByOptStringFuncName         = "zetasqlite_order_by_opt_string"
	lengthFuncName                   = "zetasqlite_length_int64"
	avgFuncName                      = "zetasqlite_avg_double"
	unitFrameFuncName                = "zetasqlite_frame_unit"
	windowStartFuncName              = "zetasqlite_window_start"
	windowEndFuncName                = "zetasqlite_window_end"
	analyticSumI64FuncName           = "zetasqlite_analytic_sum_int64"
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
		addDateFuncName:                  addDateFunc,
		subI64FuncName:                   subI64Func,
		subDateFuncName:                  subDateFunc,
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
		distinctOptFuncName:              distinctOptFunc,
		limitOptFuncName:                 limitOptFunc,
		orderByOptI64FuncName:            orderByOptI64Func,
		orderByOptStringFuncName:         orderByOptStringFunc,
		lengthFuncName:                   lengthFunc,
		unitFrameFuncName:                unitFrameFunc,
		windowStartFuncName:              windowStartFunc,
		windowEndFuncName:                windowEndFunc,
	}
	zetasqliteAggregatorFuncMap = map[string]interface{}{
		sumI64FuncName:         newSumFunc,
		analyticSumI64FuncName: newAnalyticSumFunc,
		bitAndFuncName:         newBitAndFunc,
		bitOrFuncName:          newBitOrFunc,
		bitXorFuncName:         newBitXorFunc,
		countI64FuncName:       newCountFunc,
		countStarI64FuncName:   newCountStarFunc,
		countifI64FuncName:     newCountIfFunc,
		logicalAndFuncName:     newLogicalAndFunc,
		logicalOrFuncName:      newLogicalOrFunc,
		stringAggFuncName:      newStringAggFunc,
		avgFuncName:            newAvgFunc,
	}
	builtinFunctions = []string{
		"if", "concat", "coalesce", "ifnull", "nullif", "length",
	}
	builtinAggregateFunctions = []string{
		"sum", "bit_and", "bit_or", "bit_xor",
		"count", "countif", "logical_and", "logical_or",
		"string_agg", "avg",
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

func newSumFunc() *sumFunc                 { return &sumFunc{} }
func newAnalyticSumFunc() *analyticSumFunc { return &analyticSumFunc{} }
func newBitAndFunc() *bitAndFunc           { return &bitAndFunc{-1} }
func newBitOrFunc() *bitOrFunc             { return &bitOrFunc{-1} }
func newBitXorFunc() *bitXorFunc           { return &bitXorFunc{bitXor: -1} }
func newCountFunc() *countFunc             { return &countFunc{} }
func newCountStarFunc() *countStarFunc     { return &countStarFunc{} }
func newCountIfFunc() *countIfFunc         { return &countIfFunc{} }
func newLogicalAndFunc() *logicalAndFunc   { return &logicalAndFunc{true} }
func newLogicalOrFunc() *logicalOrFunc     { return &logicalOrFunc{false} }
func newAvgFunc() *avgFunc                 { return &avgFunc{} }
func newStringAggFunc() *stringAggFunc     { return &stringAggFunc{} }

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

func addDateFunc(a, b interface{}) (string, error) {
	va, err := ValueOf(a)
	if err != nil {
		return "", err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return "", err
	}
	ret, err := va.Add(vb)
	if err != nil {
		return "", err
	}
	return ret.ToString()
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

func subDateFunc(a, b interface{}) (string, error) {
	va, err := ValueOf(a)
	if err != nil {
		return "", err
	}
	vb, err := ValueOf(b)
	if err != nil {
		return "", err
	}
	ret, err := va.Sub(vb)
	if err != nil {
		return "", err
	}
	return ret.ToString()
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

func ifI64Func(cond bool, trueV interface{}, falseV interface{}) (interface{}, error) {
	if cond {
		if isNULLValue(trueV) {
			return nil, nil
		}
		v, err := ValueOf(trueV)
		if err != nil {
			return 0, err
		}
		return v.ToInt64()
	}
	if isNULLValue(falseV) {
		return nil, nil
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

func lengthFunc(value interface{}) (int64, error) {
	if isNULLValue(value) {
		return 0, nil
	}
	v, err := ValueOf(value)
	if err != nil {
		return 0, err
	}
	s, err := v.ToString()
	if err != nil {
		return 0, err
	}
	return int64(len(s)), nil
}

const (
	unitFrameHeader   = "zetasqliteunitframe:"
	windowStartHeader = "zetasqlitewindowstart:"
	windowEndHeader   = "zetasqlitewindowend:"
)

func unitFrameFunc(frameType string) string {
	return fmt.Sprintf("%s%s", unitFrameHeader, frameType)
}

func windowStartFunc(windowType string) string {
	return fmt.Sprintf("%s%s", windowStartHeader, windowType)
}

func windowEndFunc(windowType string) string {
	return fmt.Sprintf("%s%s", windowEndHeader, windowType)
}

type analyticSumFunc struct {
	initialized bool
	once        sync.Once
	sum         int64
	aggMap      map[int64]struct{}
}

func (f *analyticSumFunc) Step(v interface{}, args ...string) error {
	if isNULLValue(v) {
		return nil
	}
	value, err := ValueOf(v)
	if err != nil {
		return err
	}
	i64, err := value.ToInt64()
	if err != nil {
		return err
	}
	f.once.Do(func() { f.initialized = true })
	distinct := parseDistinctOpt(args)
	if distinct {
		if f.aggMap == nil {
			f.aggMap = map[int64]struct{}{}
		}
		if _, exists := f.aggMap[i64]; exists {
			return nil
		}
		f.aggMap[i64] = struct{}{}
	}
	f.sum += i64
	return nil
}

func (f *analyticSumFunc) Done() interface{} {
	if !f.initialized {
		return nil
	}
	return f.sum
}

type sumFunc struct {
	initialized bool
	once        sync.Once
	sum         int64
	aggMap      map[int64]struct{}
}

func (f *sumFunc) Step(v interface{}, args ...string) error {
	if isNULLValue(v) {
		return nil
	}
	value, err := ValueOf(v)
	if err != nil {
		return err
	}
	i64, err := value.ToInt64()
	if err != nil {
		return err
	}
	f.once.Do(func() { f.initialized = true })
	distinct := parseDistinctOpt(args)
	if distinct {
		if f.aggMap == nil {
			f.aggMap = map[int64]struct{}{}
		}
		if _, exists := f.aggMap[i64]; exists {
			return nil
		}
		f.aggMap[i64] = struct{}{}
	}
	f.sum += i64
	return nil
}

func (f *sumFunc) Done() interface{} {
	if !f.initialized {
		return nil
	}
	return f.sum
}

type bitAndFunc struct {
	bitAnd int64
}

func (f *bitAndFunc) Step(v interface{}) error {
	if isNULLValue(v) {
		return nil
	}
	value, err := ValueOf(v)
	if err != nil {
		return err
	}
	i64, err := value.ToInt64()
	if err != nil {
		return err
	}
	if f.bitAnd == -1 {
		f.bitAnd = i64
	} else {
		f.bitAnd &= i64
	}
	return nil
}

func (f *bitAndFunc) Done() int64 {
	return f.bitAnd
}

type bitOrFunc struct {
	bitOr int64
}

func (f *bitOrFunc) Step(v interface{}) error {
	if isNULLValue(v) {
		return nil
	}
	value, err := ValueOf(v)
	if err != nil {
		return err
	}
	i64, err := value.ToInt64()
	if err != nil {
		return err
	}
	if f.bitOr == -1 {
		f.bitOr = i64
	} else {
		f.bitOr |= i64
	}
	return nil
}

func (f *bitOrFunc) Done() int64 {
	return f.bitOr
}

type bitXorFunc struct {
	bitXor int64
	aggMap map[int64]struct{}
}

func (f *bitXorFunc) Step(v interface{}, args ...string) error {
	if isNULLValue(v) {
		return nil
	}
	value, err := ValueOf(v)
	if err != nil {
		return err
	}
	i64, err := value.ToInt64()
	if err != nil {
		return err
	}
	distinct := parseDistinctOpt(args)
	if distinct {
		if f.aggMap == nil {
			f.aggMap = map[int64]struct{}{}
		}
		if _, exists := f.aggMap[i64]; exists {
			return nil
		}
		f.aggMap[i64] = struct{}{}
	}
	if f.bitXor == -1 {
		f.bitXor = i64
	} else {
		f.bitXor ^= i64
	}
	return nil
}

func (f *bitXorFunc) Done() int64 {
	return f.bitXor
}

type countFunc struct {
	aggMap map[int64]struct{}
	count  int64
}

func (f *countFunc) Step(v interface{}, args ...string) error {
	if isNULLValue(v) {
		return nil
	}
	value, err := ValueOf(v)
	if err != nil {
		return err
	}
	i64, err := value.ToInt64()
	if err != nil {
		return err
	}
	distinct := parseDistinctOpt(args)
	if distinct {
		if f.aggMap == nil {
			f.aggMap = map[int64]struct{}{}
		}
		if _, exists := f.aggMap[i64]; exists {
			return nil
		}
		f.aggMap[i64] = struct{}{}
	}
	f.count++
	return nil
}

func (f *countFunc) Done() int64 {
	return f.count
}

type countStarFunc struct {
	count int64
}

func (f *countStarFunc) Step() {
	f.count++
}

func (f *countStarFunc) Done() int64 {
	return f.count
}

type countIfFunc struct {
	count int64
}

func (f *countIfFunc) Step(cond bool) {
	if cond {
		f.count++
	}
}

func (f *countIfFunc) Done() int64 {
	return f.count
}

type logicalAndFunc struct {
	v bool
}

func (f *logicalAndFunc) Step(cond bool) {
	if !cond {
		f.v = false
	}
}

func (f *logicalAndFunc) Done() bool {
	return f.v
}

type logicalOrFunc struct {
	v bool
}

func (f *logicalOrFunc) Step(cond bool) {
	if cond {
		f.v = true
	}
}

func (f *logicalOrFunc) Done() bool {
	return f.v
}

type avgFunc struct {
	aggMap map[int64]struct{}
	sum    int64
	num    int64
}

func (f *avgFunc) Step(v interface{}, args ...string) error {
	if isNULLValue(v) {
		return nil
	}
	distinct := parseDistinctOpt(args)
	value, err := ValueOf(v)
	if err != nil {
		return err
	}
	i64, err := value.ToInt64()
	if err != nil {
		return err
	}
	if distinct {
		if f.aggMap == nil {
			f.aggMap = map[int64]struct{}{}
		}
		if _, exists := f.aggMap[i64]; exists {
			return nil
		}
		f.aggMap[i64] = struct{}{}
	}
	f.num++
	f.sum += i64
	return nil
}

func (f *avgFunc) Done() float64 {
	return float64(f.sum) / float64(f.num)
}

type orderedValue struct {
	value string
	opt   *orderByOpt
}

type stringAggFunc struct {
	aggMap       map[string]struct{}
	values       []*orderedValue
	delim        string
	limit        int64
	enabledSort  bool
	enabledLimit bool
}

func (f *stringAggFunc) getDelim(args []string) string {
	if len(args) == 0 {
		return ""
	}
	if isOpt(args[0]) {
		return ""
	}
	return args[0]
}

func (f *stringAggFunc) Step(v interface{}, args ...string) error {
	if isNULLValue(v) {
		return nil
	}
	f.delim = f.getDelim(args)
	distinct := parseDistinctOpt(args)
	limit, enabledLimit := parseLimitOpt(args)
	orderByOpts, err := parseOrderByOpt(args)
	if err != nil {
		return err
	}
	if len(orderByOpts) > 1 {
		return fmt.Errorf("STRING_AGG: unsupported multiple ORDER BY expression")
	}
	if enabledLimit {
		f.enabledLimit = true
		f.limit = limit
	}
	var orderBy *orderByOpt
	if len(orderByOpts) != 0 {
		f.enabledSort = true
		orderBy = orderByOpts[0]
	}

	value, err := ValueOf(v)
	if err != nil {
		return err
	}
	text, err := value.ToString()
	if err != nil {
		return err
	}
	if distinct {
		if f.aggMap == nil {
			f.aggMap = map[string]struct{}{}
		}
		if _, exists := f.aggMap[text]; exists {
			return nil
		}
		f.aggMap[text] = struct{}{}
	}
	f.values = append(f.values, &orderedValue{
		value: text,
		opt:   orderBy,
	})
	return nil
}

func (f *stringAggFunc) Done() string {
	if f.enabledSort {
		switch f.values[0].opt.typ {
		case orderByAsc:
			sort.Slice(f.values, func(i, j int) bool {
				v, _ := f.values[i].opt.value.LT(f.values[j].opt.value)
				return v
			})
		case orderByDsc:
			sort.Slice(f.values, func(i, j int) bool {
				v, _ := f.values[i].opt.value.GT(f.values[j].opt.value)
				return v
			})
		}
	}
	if f.enabledLimit {
		minLen := int64(len(f.values))
		if f.limit < minLen {
			minLen = f.limit
		}
		f.values = f.values[:minLen]
	}
	values := make([]string, 0, len(f.values))
	for _, v := range f.values {
		values = append(values, v.value)
	}
	delim := f.delim
	if delim == "" {
		delim = ","
	}
	return strings.Join(values, delim)
}

func decodeArrayFunc(v string) (string, error) {
	json, err := jsonArrayFromEncodedString(v)
	if err != nil {
		return "", err
	}
	return string(json), nil
}

const (
	distinctOptHeader = "zetasqlitedistinct:"
	limitOptHeader    = "zetasqlitelimit:"
	orderByOptHeader  = "zetasqliteorderby:"
)

func distinctOptFunc() string {
	return encodeDistinctOpt(true)
}

func limitOptFunc(limit int64) string {
	return encodeLimitOpt(limit)
}

func orderByOptI64Func(value int64, isAsc bool) string {
	if isAsc {
		return encodeOrderByI64Opt(value, orderByAsc)
	}
	return encodeOrderByI64Opt(value, orderByDsc)
}

func orderByOptStringFunc(value string, isAsc bool) string {
	if isAsc {
		return encodeOrderByStringOpt(value, orderByAsc)
	}
	return encodeOrderByStringOpt(value, orderByDsc)
}

func isOpt(opt string) bool {
	switch {
	case strings.HasPrefix(opt, distinctOptHeader):
		return true
	case strings.HasPrefix(opt, limitOptHeader):
		return true
	case strings.HasPrefix(opt, orderByOptHeader):
		return true
	}
	return false
}

func encodeDistinctOpt(isDistinct bool) string {
	return fmt.Sprintf("%s%t", distinctOptHeader, isDistinct)
}

func decodeDistinctOpt(opt string) bool {
	if strings.HasPrefix(opt, distinctOptHeader) {
		b, _ := strconv.ParseBool(opt[len(distinctOptHeader):])
		return b
	}
	return false
}

func parseDistinctOpt(opts []string) bool {
	for _, opt := range opts {
		if decodeDistinctOpt(opt) {
			return true
		}
	}
	return false
}

func encodeLimitOpt(limit int64) string {
	return fmt.Sprintf("%s%d", limitOptHeader, limit)
}

func decodeLimitOpt(opt string) int64 {
	if strings.HasPrefix(opt, limitOptHeader) {
		i, _ := strconv.ParseInt(opt[len(limitOptHeader):], 10, 64)
		return i
	}
	return 0
}

func parseLimitOpt(opts []string) (int64, bool) {
	for _, opt := range opts {
		limit := decodeLimitOpt(opt)
		if limit > 0 {
			return limit, true
		}
	}
	return 0, false
}

type orderByType string

const (
	orderByAsc orderByType = "asc"
	orderByDsc orderByType = "dsc"
)

type orderByOpt struct {
	typ   orderByType
	value Value
}

func encodeOrderByI64Opt(value int64, typ orderByType) string {
	return fmt.Sprintf("%si:%s:%d", orderByOptHeader, typ, value)
}

func encodeOrderByStringOpt(value string, typ orderByType) string {
	return fmt.Sprintf("%ss:%s:%s", orderByOptHeader, typ, value)
}

func decodeOrderByI64Opt(opt string) (*orderByOpt, error) {
	if !strings.HasPrefix(opt, orderByOptHeader) {
		return nil, nil
	}
	removedHeader := opt[len(orderByOptHeader):]
	if removedHeader[0] != 'i' {
		return nil, fmt.Errorf("order by option is not int64 type")
	}
	removedTypeInfo := removedHeader[1:]
	orderType := orderByType(removedTypeInfo[1:4]) // asc or dsc
	i64, err := strconv.ParseInt(removedTypeInfo[5:], 10, 64)
	if err != nil {
		return nil, err
	}
	return &orderByOpt{
		typ:   orderType,
		value: IntValue(i64),
	}, nil
}

func decodeOrderByStringOpt(opt string) (*orderByOpt, error) {
	if !strings.HasPrefix(opt, orderByOptHeader) {
		return nil, nil
	}
	removedHeader := opt[len(orderByOptHeader):]
	if removedHeader[0] != 's' {
		return nil, fmt.Errorf("order by option is not string type")
	}
	removedTypeInfo := removedHeader[1:]
	orderType := orderByType(removedTypeInfo[1:4]) // asc or dsc
	return &orderByOpt{
		typ:   orderType,
		value: StringValue(removedTypeInfo[5:]),
	}, nil
}

func parseOrderByOpt(opts []string) ([]*orderByOpt, error) {
	var ret []*orderByOpt
	for _, opt := range opts {
		if !strings.HasPrefix(opt, orderByOptHeader) {
			continue
		}
		removedHeader := opt[len(orderByOptHeader):]
		switch removedHeader[0] {
		case 'i':
			// int64 type
			v, err := decodeOrderByI64Opt(opt)
			if err != nil {
				return nil, err
			}
			if v != nil {
				ret = append(ret, v)
			}
		case 's':
			// string type
			v, err := decodeOrderByStringOpt(opt)
			if err != nil {
				return nil, err
			}
			if v != nil {
				ret = append(ret, v)
			}
		}
	}
	return ret, nil
}
