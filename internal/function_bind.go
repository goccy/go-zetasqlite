package internal

import (
	"fmt"

	"github.com/goccy/go-zetasql/types"
)

type SQLiteFunction func(...interface{}) (interface{}, error)
type BindFunction func(...Value) (Value, error)
type AggregateBindFunction func(ReturnValueConverter) func() *Aggregator
type WindowBindFunction func(ReturnValueConverter) func() *WindowAggregator

type FuncInfo struct {
	Name        string
	BindFunc    BindFunction
	ReturnTypes []types.TypeKind
}

type AggregateFuncInfo struct {
	Name        string
	BindFunc    AggregateBindFunction
	ReturnTypes []types.TypeKind
}

type WindowFuncInfo struct {
	Name        string
	BindFunc    WindowBindFunction
	ReturnTypes []types.TypeKind
}

func convertArgs(args ...interface{}) ([]Value, error) {
	values := make([]Value, 0, len(args))
	for _, arg := range args {
		v, err := ValueOf(arg)
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, nil
}

func bindIntFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToInt64()
	}
}

func bindFloatFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToFloat64()
	}
}

func bindStringFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToString()
	}
}

func bindBoolFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToBool()
	}
}

func bindDateFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToString()
	}
}

func bindArrayFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToString()
	}
}

func bindStructFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToString()
	}
}

type ReturnValueConverter func(Value) (interface{}, error)

var (
	intValueConverter = func(v Value) (interface{}, error) {
		if v == nil {
			return nil, nil
		}
		return v.ToInt64()
	}
	floatValueConverter = func(v Value) (interface{}, error) {
		if v == nil {
			return nil, nil
		}
		return v.ToFloat64()
	}
	stringValueConverter = func(v Value) (interface{}, error) {
		if v == nil {
			return nil, nil
		}
		return v.ToString()
	}
	boolValueConverter = func(v Value) (interface{}, error) {
		if v == nil {
			return nil, nil
		}
		return v.ToBool()
	}
)

func bindAggregateIntFunc(bindFunc func(ReturnValueConverter) func() *Aggregator) func() *Aggregator {
	return bindFunc(intValueConverter)
}

func bindAggregateFloatFunc(bindFunc func(ReturnValueConverter) func() *Aggregator) func() *Aggregator {
	return bindFunc(floatValueConverter)
}

func bindAggregateStringFunc(bindFunc func(ReturnValueConverter) func() *Aggregator) func() *Aggregator {
	return bindFunc(stringValueConverter)
}

func bindAggregateBoolFunc(bindFunc func(ReturnValueConverter) func() *Aggregator) func() *Aggregator {
	return bindFunc(boolValueConverter)
}

type Aggregator struct {
	distinctMap map[string]struct{}
	step        func([]Value, *AggregatorOption) error
	done        func() (Value, error)
	converter   ReturnValueConverter
}

func (a *Aggregator) Step(stepArgs ...interface{}) error {
	args, opt, err := parseAggregateOptions(stepArgs...)
	if err != nil {
		return err
	}
	values, err := convertArgs(args...)
	if err != nil {
		return err
	}
	if opt.Distinct {
		if len(values) < 1 {
			return fmt.Errorf("DISTINCT option required at least one argument")
		}
		if values[0] == nil {
			// if value is nil, ignore it.
			return nil
		}
		key, err := values[0].ToString()
		if err != nil {
			return err
		}
		if _, exists := a.distinctMap[key]; exists {
			return nil
		}
		a.distinctMap[key] = struct{}{}
	}
	return a.step(values, opt)
}

func (a *Aggregator) Done() (interface{}, error) {
	ret, err := a.done()
	if err != nil {
		return nil, err
	}
	return a.converter(ret)
}

func newAggregator(
	step func([]Value, *AggregatorOption) error,
	done func() (Value, error),
	converter ReturnValueConverter) *Aggregator {
	return &Aggregator{
		distinctMap: map[string]struct{}{},
		step:        step,
		done:        done,
		converter:   converter,
	}
}

func bindWindowIntFunc(bindFunc func(ReturnValueConverter) func() *WindowAggregator) func() *WindowAggregator {
	return bindFunc(intValueConverter)
}

func bindWindowFloatFunc(bindFunc func(ReturnValueConverter) func() *WindowAggregator) func() *WindowAggregator {
	return bindFunc(floatValueConverter)
}

func bindWindowStringFunc(bindFunc func(ReturnValueConverter) func() *WindowAggregator) func() *WindowAggregator {
	return bindFunc(stringValueConverter)
}

func bindWindowBoolFunc(bindFunc func(ReturnValueConverter) func() *WindowAggregator) func() *WindowAggregator {
	return bindFunc(boolValueConverter)
}

type WindowAggregator struct {
	distinctMap map[string]struct{}
	agg         *WindowFuncAggregatedStatus
	step        func([]Value, *AggregatorOption, *WindowFuncStatus, *WindowFuncAggregatedStatus) error
	done        func(*WindowFuncAggregatedStatus) (Value, error)
	converter   ReturnValueConverter
}

func (a *WindowAggregator) Step(stepArgs ...interface{}) error {
	args, opt, err := parseAggregateOptions(stepArgs...)
	if err != nil {
		return err
	}
	newArgs, windowOpt, err := parseWindowOptions(args...)
	if err != nil {
		return err
	}
	values, err := convertArgs(newArgs...)
	if err != nil {
		return err
	}
	if opt.Distinct {
		if len(values) < 1 {
			return fmt.Errorf("DISTINCT option required at least one argument")
		}
		key, err := values[0].ToString()
		if err != nil {
			return err
		}
		if _, exists := a.distinctMap[key]; exists {
			return nil
		}
		a.distinctMap[key] = struct{}{}
	}
	return a.step(values, opt, windowOpt, a.agg)
}

func (a *WindowAggregator) Done() (interface{}, error) {
	ret, err := a.done(a.agg)
	if err != nil {
		return nil, err
	}
	return a.converter(ret)
}

func newWindowAggregator(
	step func([]Value, *AggregatorOption, *WindowFuncStatus, *WindowFuncAggregatedStatus) error,
	done func(*WindowFuncAggregatedStatus) (Value, error),
	converter ReturnValueConverter) *WindowAggregator {
	return &WindowAggregator{
		distinctMap: map[string]struct{}{},
		agg:         newWindowFuncAggregatedStatus(),
		step:        step,
		done:        done,
		converter:   converter,
	}
}

func bindAdd(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ADD: invalid argument num %d", len(args))
	}
	return ADD(args[0], args[1])
}

func bindSub(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("SUB: invalid argument num %d", len(args))
	}
	return SUB(args[0], args[1])
}

func bindMul(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("MUL: invalid argument num %d", len(args))
	}
	return MUL(args[0], args[1])
}

func bindDiv(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("DIV: invalid argument num %d", len(args))
	}
	return DIV(args[0], args[1])
}

func bindEqual(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("EQ: invalid argument num %d", len(args))
	}
	return EQ(args[0], args[1])
}

func bindNotEqual(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("NOT_EQ: invalid argument num %d", len(args))
	}
	return NOT_EQ(args[0], args[1])
}

func bindGreater(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("GT: invalid argument num %d", len(args))
	}
	return GT(args[0], args[1])
}

func bindGreaterOrEqual(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("GT: invalid argument num %d", len(args))
	}
	return GTE(args[0], args[1])
}

func bindLess(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("LT: invalid argument num %d", len(args))
	}
	return LT(args[0], args[1])
}

func bindLessOrEqual(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("LTE: invalid argument num %d", len(args))
	}
	return LTE(args[0], args[1])
}

func bindBitNot(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("BIT_NOT: invalid argument num %d", len(args))
	}
	return BIT_NOT(args[0])
}

func bindBitLeftShift(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("BIT_LEFT_SHIFT: invalid argument num %d", len(args))
	}
	return BIT_LEFT_SHIFT(args[0], args[1])
}

func bindBitRightShift(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("BIT_RIGHT_SHIFT: invalid argument num %d", len(args))
	}
	return BIT_RIGHT_SHIFT(args[0], args[1])
}

func bindBitAnd(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("BIT_AND: invalid argument num %d", len(args))
	}
	return BIT_AND(args[0], args[1])
}

func bindBitOr(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("BIT_OR: invalid argument num %d", len(args))
	}
	return BIT_OR(args[0], args[1])
}

func bindBitXor(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("BIT_XOR: invalid argument num %d", len(args))
	}
	return BIT_XOR(args[0], args[1])
}

func bindInArray(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ARRAY_IN: invalid argument num %d", len(args))
	}
	return ARRAY_IN(args[0], args[1])
}

func bindStructField(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("STRUCT_FIELD: invalid argument num %d", len(args))
	}
	i64, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return STRUCT_FIELD(args[0], int(i64))
}

func bindArrayAtOffset(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ARRAY_OFFSET: invalid argument num %d", len(args))
	}
	i64, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return ARRAY_OFFSET(args[0], int(i64))
}

func bindSafeArrayAtOffset(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ARRAY_SAFE_OFFSET: invalid argument num %d", len(args))
	}
	i64, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return ARRAY_SAFE_OFFSET(args[0], int(i64))
}

func bindArrayAtOrdinal(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ARRAY_ORDINAL: invalid argument num %d", len(args))
	}
	i64, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return ARRAY_ORDINAL(args[0], int(i64))
}

func bindSafeArrayAtOrdinal(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ARRAY_SAFE_ORDINAL: invalid argument num %d", len(args))
	}
	i64, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return ARRAY_SAFE_ORDINAL(args[0], int(i64))
}

func bindConcat(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("CONCAT: invalid argument num %d", len(args))
	}
	return CONCAT(args[0], args[1])
}

func bindLike(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("LIKE: invalid argument num %d", len(args))
	}
	return LIKE(args[0], args[1])
}

func bindBetween(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("BETWEEN: invalid argument num %d", len(args))
	}
	return BETWEEN(args[0], args[1], args[2])
}

func bindIn(args ...Value) (Value, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("IN: invalid argument num %d", len(args))
	}
	return IN(args[0], args[1:]...)
}

func bindIsNull(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("IS_NULL: invalid argument num %d", len(args))
	}
	return IS_NULL(args[0])
}

func bindIsTrue(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("IS_TRUE: invalid argument num %d", len(args))
	}
	return IS_TRUE(args[0])
}

func bindIsFalse(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("IS_FALSE: invalid argument num %d", len(args))
	}
	return IS_FALSE(args[0])
}

func bindNot(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("NOT: invalid argument num %d", len(args))
	}
	return NOT(args[0])
}

func bindAnd(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("AND: invalid argument num %d", len(args))
	}
	return AND(args[0], args[1])
}

func bindOr(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("OR: invalid argument num %d", len(args))
	}
	return OR(args[0], args[1])
}

func bindCaseWithValue(args ...Value) (Value, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("CASE_WITH_VALUE: invalid argument num %d", len(args))
	}
	return CASE_WITH_VALUE(args[0], args[1:]...)
}

func bindCaseNoValue(args ...Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("CASE_NO_VALUE: invalid argument num %d", len(args))
	}
	return CASE_NO_VALUE(args...)
}

func bindCoalesce(args ...Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("COALESCE: invalid argument num %d", len(args))
	}
	return COALESCE(args...)
}

func bindIf(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("IF: invalid argument num %d", len(args))
	}
	cond, err := args[0].ToBool()
	if err != nil {
		return nil, err
	}
	return IF(cond, args[1], args[2])
}

func bindIfNull(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("IFNULL: invalid argument num %d", len(args))
	}
	return IFNULL(args[0], args[1])
}

func bindNullIf(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("NULLIF: invalid argument num %d", len(args))
	}
	return NULLIF(args[0], args[1])
}

func bindLength(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LENGTH: invalid argument num %d", len(args))
	}
	return LENGTH(args[0])
}

func bindDate(args ...Value) (Value, error) {
	return DATE(args...)
}

func bindDecodeArray(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("DECODE_ARRAY: invalid argument num %d", len(args))
	}
	s, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return DECODE_ARRAY(s)
}

func bindDistinct(args ...Value) (Value, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("DISTINCT: invalid argument num %d", len(args))
	}
	return DISTINCT()
}

func bindLimit(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LIMIT: invalid argument num %d", len(args))
	}
	i64, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return LIMIT(i64)
}

func bindOrderBy(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ORDER_BY: invalid argument num %d", len(args))
	}
	b, err := args[1].ToBool()
	if err != nil {
		return nil, err
	}
	return ORDER_BY(args[0], b)
}

func bindWindowFrameUnit(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("WINDOW_FRAME_UNIT: invalid argument num %d", len(args))
	}
	i64, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return WINDOW_FRAME_UNIT(i64)
}

func bindWindowPartition(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("WINDOW_PARTITION: invalid argument num %d", len(args))
	}
	return WINDOW_PARTITION(args[0])
}

func bindWindowBoundaryStart(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("WINDOW_BOUNDARY_START: invalid argument num %d", len(args))
	}
	a0, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	a1, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return WINDOW_BOUNDARY_START(a0, a1)
}

func bindWindowBoundaryEnd(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("WINDOW_BOUNDARY_END: invalid argument num %d", len(args))
	}
	a0, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	a1, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return WINDOW_BOUNDARY_END(a0, a1)
}

func bindWindowRowID(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("WINDOW_ROWID: invalid argument num %d", len(args))
	}
	a0, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return WINDOW_ROWID(a0)
}

func bindWindowOrderBy(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("WINDOW_ORDER_BY: invalid argument num %d", len(args))
	}
	return WINDOW_ORDER_BY(args[0])
}

func bindSum(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &SUM{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("SUM: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindAvg(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &AVG{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("AVG: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindCount(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &COUNT{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("COUNT: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindCountStar(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &COUNT_STAR{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 0 {
					return fmt.Errorf("COUNT_STAR: invalid argument num %d", len(args))
				}
				return fn.Step(opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindBitAndAgg(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &BIT_AND_AGG{IntValue(-1)}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("BIT_AND_AGG: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindBitOrAgg(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &BIT_OR_AGG{-1}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("BIT_OR_AGG: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindBitXorAgg(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &BIT_XOR_AGG{1}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("BIT_XOR_AGG: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindCountIf(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &COUNTIF{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("COUNT_IF: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindLogicalAnd(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &LOGICAL_AND{true}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("LOGICAL_AND: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindLogicalOr(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &LOGICAL_OR{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("LOGICAL_OR: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindStringAgg(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &STRING_AGG{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 && len(args) != 2 {
					return fmt.Errorf("STRING_AGG: invalid argument num %d", len(args))
				}
				if len(args) == 1 {
					return fn.Step(args[0], "", opt)
				}
				delim, err := args[1].ToString()
				if err != nil {
					return err
				}
				return fn.Step(args[0], delim, opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindWindowSum(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_SUM{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 {
					return fmt.Errorf("WINDOW_SUM: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowCountStar(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_COUNT_STAR{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 0 {
					return fmt.Errorf("WINDOW_COUNT_STAR: invalid argument num %d", len(args))
				}
				return fn.Step(windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowAvg(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_AVG{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 {
					return fmt.Errorf("WINDOW_AVG: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowLastValue(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_LAST_VALUE{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 {
					return fmt.Errorf("WINDOW_LAST_VALUE: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowRank(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_RANK{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 0 {
					return fmt.Errorf("WINDOW_RANK: invalid argument num %d", len(args))
				}
				return fn.Step(windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}
