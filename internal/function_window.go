package internal

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"gonum.org/v1/gonum/stat"
)

type WINDOW_ANY_VALUE struct {
}

func (f *WINDOW_ANY_VALUE) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	if len(agg.Values) == 0 {
		return nil, nil
	}
	return agg.Values[0], nil
}

type WINDOW_ARRAY_AGG struct {
}

func (f *WINDOW_ARRAY_AGG) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	ret := &ArrayValue{}
	ret.values, _ = agg.RelevantValues()
	return ret, nil
}

type WINDOW_AVG struct {
}

func (f *WINDOW_AVG) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var avg Value

	var sum Value
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	total := 0
	for _, value := range values {
		if value == nil {
			continue
		}
		total += 1
		if sum == nil {
			f64, err := value.ToFloat64()
			if err != nil {
				return nil, err
			}
			sum = FloatValue(f64)
		} else {
			added, err := sum.Add(value)
			if err != nil {
				return nil, err
			}
			sum = added
		}
	}
	if sum == nil {
		return nil, nil
	}
	ret, err := sum.Div(FloatValue(float64(total)))
	if err != nil {
		return nil, err
	}
	avg = ret
	return avg, nil
}

type WINDOW_COUNT struct {
}

func (f *WINDOW_COUNT) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	return IntValue(len(values)), nil
}

type WINDOW_COUNT_STAR struct {
}

func (f *WINDOW_COUNT_STAR) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	return IntValue(len(values)), nil
}

type WINDOW_COUNTIF struct {
}

func (f *WINDOW_COUNTIF) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var count int64
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		if value == nil {
			continue
		}
		cond, err := value.ToBool()
		if err != nil {
			return nil, err
		}
		if cond {
			count++
		}
	}
	return IntValue(count), nil
}

type WINDOW_MAX struct {
}

func (f *WINDOW_MAX) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var (
		max Value
	)
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		if value == nil {
			continue
		}
		if max == nil {
			max = value
		} else {
			cond, err := value.GT(max)
			if err != nil {
				return nil, err
			}
			if cond {
				max = value
			}
		}
	}
	return max, nil
}

type WINDOW_MIN struct {
}

func (f *WINDOW_MIN) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var (
		min Value
	)
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		if value == nil {
			continue
		}
		if min == nil {
			min = value
		} else {
			cond, err := value.LT(min)
			if err != nil {
				return nil, err
			}
			if cond {
				min = value
			}
		}

	}
	return min, nil
}

type WINDOW_STRING_AGG struct {
	delim string
}

func (f *WINDOW_STRING_AGG) ParseArguments(args []Value) error {
	f.delim = ","
	if len(args) > 1 {
		d, err := args[1].ToString()
		if err != nil {
			return err
		}
		f.delim = d
	}
	return nil
}

func (f *WINDOW_STRING_AGG) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var strValues []string
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		if value == nil {
			continue
		}
		text, err := value.ToString()
		if err != nil {
			return nil, err
		}
		strValues = append(strValues, text)
	}
	if len(strValues) == 0 {
		return nil, nil
	}
	return StringValue(strings.Join(strValues, f.delim)), nil
}

type WINDOW_SUM struct {
}

func (f *WINDOW_SUM) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var sum Value
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		if value == nil {
			continue
		}
		if sum == nil {
			sum = value
		} else {
			added, err := sum.Add(value)
			if err != nil {
				return nil, err
			}
			sum = added
		}
	}
	return sum, nil
}

type WINDOW_FIRST_VALUE struct {
}

func (f *WINDOW_FIRST_VALUE) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, nil
	}
	return values[0], nil
}

type WINDOW_LAST_VALUE struct {
}

func (f *WINDOW_LAST_VALUE) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, nil
	}
	return values[len(values)-1], nil
}

type WINDOW_LEAD struct {
	offset       int
	defaultValue Value
}

func (f *WINDOW_LEAD) ParseArguments(args []Value) error {
	if len(args) > 3 {
		return fmt.Errorf("LEAD: expected at most 3 arguments; got [%d]", len(args))
	}

	// Defaults
	f.offset = 1
	f.defaultValue = nil

	for i := range args {
		arg := args[i]

		switch i {
		case 0:
			continue
		case 1:
			if arg == nil {
				return fmt.Errorf("LEAD: constant integer expression must be not null value")
			}

			offset, err := arg.ToInt64()
			if err != nil {
				return fmt.Errorf("LEAD: %w", err)
			}
			if offset < 0 {
				return fmt.Errorf("LEAD: Argument 2 to LEAD must be at least 0; got %d", offset)
			}
			// offset uses ordinal access
			f.offset = int(offset)
		case 2:
			f.defaultValue = arg
		}
	}
	return nil
}

func (f *WINDOW_LEAD) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	// Values includes the current row, so offset is 1 + f.offset
	if len(agg.Values)-1 < f.offset {
		return f.defaultValue, nil
	}
	return agg.Values[f.offset], nil
}

type WINDOW_NTH_VALUE struct {
	once sync.Once
	n    int
}

func (f *WINDOW_NTH_VALUE) ParseArguments(args []Value) error {
	if args[1] == nil {
		return fmt.Errorf("NTH_VALUE: constant integer expression must be not null value")
	}
	n, err := args[1].ToInt64()
	if err != nil {
		return fmt.Errorf("NTH_VALUE: %w", err)
	}
	// n uses ordinal access
	f.n = int(n) - 1
	return nil
}

func (f *WINDOW_NTH_VALUE) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	if len(values)-1 < f.n {
		return nil, nil
	}
	return values[f.n], nil
}

type WINDOW_LAG struct {
	offset       int
	defaultValue Value
}

func (f *WINDOW_LAG) ParseArguments(args []Value) error {
	if len(args) > 3 {
		return fmt.Errorf("LEAD: expected at most 3 arguments; got [%d]", len(args))
	}
	// Defaults
	f.offset = 1
	f.defaultValue = nil

	for i := range args {
		arg := args[i]

		switch i {
		case 0:
			continue
		case 1:
			if arg == nil {
				return fmt.Errorf("LAG: constant integer expression must be not null value")
			}
			offset, err := arg.ToInt64()
			if err != nil {
				return fmt.Errorf("LAG: %w", err)
			}
			if offset < 0 {
				return fmt.Errorf("LAG: Argument 2 to LAG must be at least 0; got %d", offset)
			}
			// offset uses ordinal access
			f.offset = int(offset)
		case 2:
			f.defaultValue = arg
		}
	}
	return nil
}

func (f *WINDOW_LAG) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	// Values includes the current row, so offset is f.offset - 1
	if len(agg.Values)-1 < f.offset {
		return f.defaultValue, nil
	}
	return agg.Values[len(agg.Values)-f.offset-1], nil
}

type WINDOW_PERCENTILE_CONT struct {
	percentile Value
}

func (f *WINDOW_PERCENTILE_CONT) ParseArguments(args []Value) error {
	f.percentile = args[1]
	return nil
}

func (f *WINDOW_PERCENTILE_CONT) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	if cond, _ := f.percentile.LT(IntValue(0)); cond {
		return nil, fmt.Errorf("PERCENTILE_CONT: percentile value must be greater than zero")
	}
	if cond, _ := f.percentile.GT(IntValue(1)); cond {
		return nil, fmt.Errorf("PERCENTILE_CONT: percentile value must be less than one")
	}
	var (
		maxValue         Value
		minValue         Value
		floorValue       Value
		ceilingValue     Value
		rowNumber        float64
		floorRowNumber   float64
		ceilingRowNumber float64
		nonNullValues    []int
	)
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, nil
	}
	var filteredValues []Value
	values, err = agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		if value == nil {
			continue
		}
		int64Val, err := value.ToInt64()
		if err != nil {
			return nil, err
		}
		nonNullValues = append(nonNullValues, int(int64Val))
		filteredValues = append(filteredValues, value)
	}
	if len(filteredValues) == 0 {
		return nil, nil
	}

	// Calculate row number at percentile
	percentile, err := f.percentile.ToFloat64()
	if err != nil {
		return nil, err
	}
	sort.Ints(nonNullValues)

	// rowNumber = (1 + (percentile * (length of array - 1)
	rowNumber = 1 + percentile*float64(len(nonNullValues)-1)
	floorRowNumber = math.Floor(rowNumber)
	floorValue = FloatValue(nonNullValues[int(floorRowNumber-1)])
	ceilingRowNumber = math.Ceil(rowNumber)
	ceilingValue = FloatValue(nonNullValues[int(ceilingRowNumber-1)])

	maxValue = filteredValues[0]
	minValue = filteredValues[0]
	for _, value := range filteredValues {
		if value == nil {
			// TODO: support RESPECT NULLS
			continue
		}
		if maxValue == nil {
			maxValue = value
		}
		if minValue == nil {
			minValue = value
		}
		if cond, _ := value.GT(maxValue); cond {
			maxValue = value
		}
		if cond, _ := value.LT(minValue); cond {
			minValue = value
		}
	}
	if maxValue == nil || minValue == nil {
		return nil, nil
	}
	if cond, _ := maxValue.EQ(IntValue(0)); cond {
		return FloatValue(0), nil
	}

	// if ceilingRowNumber = floorRowNumber = rowNumber, return value at rownNumber which is equivalent of floorValue
	if ceilingRowNumber == floorRowNumber && ceilingRowNumber == rowNumber {
		return floorValue, nil
	}

	// (value of row at ceilingRowNumber) * (rowNumber – floorRowNumber) +
	// (value of row at floorRowNumber) * (ceilingRowNumber – rowNumber)
	leftSide, err := ceilingValue.Mul(FloatValue(rowNumber - floorRowNumber))
	if err != nil {
		return nil, err
	}
	rightSide, err := floorValue.Mul(FloatValue(ceilingRowNumber - rowNumber))
	if err != nil {
		return nil, err
	}

	ret, err := leftSide.Add(rightSide)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

type WINDOW_PERCENTILE_DISC struct {
	percentile Value
}

func (f *WINDOW_PERCENTILE_DISC) ParseArguments(args []Value) error {
	f.percentile = args[1]
	return nil
}

func (f *WINDOW_PERCENTILE_DISC) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	if cond, _ := f.percentile.LT(IntValue(0)); cond {
		return nil, fmt.Errorf("PERCENTILE_DISC: percentile value must be greater than zero")
	}
	if cond, _ := f.percentile.GT(IntValue(1)); cond {
		return nil, fmt.Errorf("PERCENTILE_DISC: percentile value must be less than one")
	}
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, nil
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i] == nil {
			return true
		}
		if values[j] == nil {
			return false
		}
		cond, _ := values[i].LT(values[j])
		return cond
	})
	pickPoint, err := f.percentile.Mul(IntValue(len(values)))
	if err != nil {
		return nil, err
	}
	if cond, _ := pickPoint.EQ(IntValue(0)); cond {
		return values[0], nil
	}
	fIdx, err := pickPoint.ToFloat64()
	if err != nil {
		return nil, err
	}
	idx := int64(fIdx)
	if float64(idx) < fIdx {
		idx += 1
	}
	idx -= 1
	if idx > 0 {
		return values[idx], nil
	}
	return nil, nil
}

// WINDOW_RANK is implemented by deferring windowing to SQLite
// See windowFuncFixedRanges["zetasqlite_window_rank"]
type WINDOW_RANK struct {
}

func (f *WINDOW_RANK) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	return IntValue(len(values)), nil

}

// WINDOW_DENSE_RANK is implemented by deferring windowing to SQLite
// See windowFuncFixedRanges["zetasqlite_window_dense_rank"]
type WINDOW_DENSE_RANK struct {
	nStep  int
	nTotal int
}

func (f *WINDOW_DENSE_RANK) Step(values []Value, agg *WindowFuncAggregatedStatus) error {
	f.nStep = 1
	return nil
}

func (f *WINDOW_DENSE_RANK) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	if f.nStep != 0 {
		f.nTotal++
	}
	return IntValue(f.nTotal), nil
}

type WINDOW_PERCENT_RANK struct {
	nStep  int
	nTotal int
	nValue int
}

func (f *WINDOW_PERCENT_RANK) Step(args []Value, agg *WindowFuncAggregatedStatus) error {
	f.nTotal++
	return nil
}

func (f *WINDOW_PERCENT_RANK) Inverse(args []Value, agg *WindowFuncAggregatedStatus) error {
	f.nStep++
	return nil
}

func (f *WINDOW_PERCENT_RANK) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	f.nValue = f.nStep
	if f.nTotal > 1 {
		return FloatValue(float64(f.nValue) / float64(f.nTotal-1)), nil
	}
	return FloatValue(0.0), nil
}

type WINDOW_CUME_DIST struct {
	nStep  int
	nTotal int
}

func (f *WINDOW_CUME_DIST) Step(values []Value, agg *WindowFuncAggregatedStatus) error {
	f.nTotal++
	return nil
}

func (f *WINDOW_CUME_DIST) Inverse(values []Value, agg *WindowFuncAggregatedStatus) error {
	f.nStep++
	return nil
}

func (f *WINDOW_CUME_DIST) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	return FloatValue(float64(f.nStep) / float64(f.nTotal)), nil
}

type WINDOW_NTILE struct {
	nParam int64
	nTotal int64
	nStep  int64
	iRow   int64
}

func (f *WINDOW_NTILE) ParseArguments(args []Value) error {
	if len(args) < 1 {
		return fmt.Errorf("NTILE: must provide one argument")
	}
	if args[0] == nil {
		return fmt.Errorf("NTILE: constant integer expression must not be null value")
	}
	value, err := args[0].ToInt64()
	if err != nil {
		return fmt.Errorf("NTILE: error parsing argument: %s", err)
	}
	if value <= 0 {
		return fmt.Errorf("NTILE: constant integer expression must be positive value")
	}
	f.nParam = value
	return nil
}

func (f *WINDOW_NTILE) Step(values []Value, agg *WindowFuncAggregatedStatus) error {
	f.nTotal++
	return nil
}

func (f *WINDOW_NTILE) Inverse(values []Value, agg *WindowFuncAggregatedStatus) error {
	f.iRow++
	return nil
}

func (f *WINDOW_NTILE) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	nSize := f.nTotal / f.nParam
	if nSize == 0 {
		return IntValue(f.iRow + 1), nil
	} else {
		nLarge := f.nTotal - f.nParam*nSize
		iSmall := nLarge * (nSize + 1)
		if (nLarge*(nSize+1) + (f.nParam-nLarge)*nSize) != f.nTotal {
			return nil, fmt.Errorf("assertion failed")
		}
		if f.iRow < iSmall {
			return IntValue(1 + f.iRow/(nSize+1)), nil
		} else {
			return IntValue(1 + nLarge + (f.iRow-iSmall)/nSize), nil
		}
	}
}

type WINDOW_ROW_NUMBER struct {
}

func (f *WINDOW_ROW_NUMBER) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	return IntValue(len(agg.Values)), nil
}

type WINDOW_CORR struct {
}

func (f *WINDOW_CORR) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var (
		x []float64
		y []float64
	)
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	if len(values) < 2 {
		return nil, nil
	}
	for _, value := range values {
		arr, err := value.ToArray()
		if err != nil {
			return nil, err
		}
		if len(arr.values) != 2 {
			return nil, fmt.Errorf("invalid corr arguments")
		}
		x1, err := arr.values[0].ToFloat64()
		if err != nil {
			return nil, err
		}
		x2, err := arr.values[1].ToFloat64()
		if err != nil {
			return nil, err
		}
		x = append(x, x1)
		y = append(y, x2)
	}

	if len(x) == 0 || len(y) == 0 {
		return nil, nil
	}
	return FloatValue(stat.Correlation(x, y, nil)), nil
}

type WINDOW_COVAR_POP struct {
}

func (f *WINDOW_COVAR_POP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var (
		x []float64
		y []float64
	)
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	if len(values) < 2 {
		return nil, nil
	}
	for _, value := range values {
		arr, err := value.ToArray()
		if err != nil {
			return nil, err
		}
		if len(arr.values) != 2 {
			return nil, fmt.Errorf("invalid covar_pop arguments")
		}
		x1, err := arr.values[0].ToFloat64()
		if err != nil {
			return nil, err
		}
		x2, err := arr.values[1].ToFloat64()
		if err != nil {
			return nil, err
		}
		x = append(x, x1)
		y = append(y, x2)
	}
	if len(x) == 0 || len(y) == 0 {
		return nil, nil
	}
	// TODO(goccy/go-zetasqlite#168): Use population covariance instead of sample covariance
	return FloatValue(stat.Covariance(x, y, nil)), nil
}

type WINDOW_COVAR_SAMP struct {
}

func (f *WINDOW_COVAR_SAMP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var (
		x []float64
		y []float64
	)
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	if len(values) < 2 {
		return nil, nil
	}
	for _, value := range values {
		arr, err := value.ToArray()
		if err != nil {
			return nil, err
		}
		if len(arr.values) != 2 {
			return nil, fmt.Errorf("invalid covar_samp arguments")
		}
		x1, err := arr.values[0].ToFloat64()
		if err != nil {
			return nil, err
		}
		x2, err := arr.values[1].ToFloat64()
		if err != nil {
			return nil, err
		}
		x = append(x, x1)
		y = append(y, x2)
	}
	if len(x) == 0 || len(y) == 0 {
		return nil, nil
	}
	return FloatValue(stat.Covariance(x, y, nil)), nil
}

type WINDOW_STDDEV_POP struct {
}

func (f *WINDOW_STDDEV_POP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var stddevpop []float64
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	if len(values) < 2 {
		return nil, nil
	}
	for _, value := range values {
		f64, err := value.ToFloat64()
		if err != nil {
			return nil, err
		}
		stddevpop = append(stddevpop, f64)
	}
	if len(stddevpop) == 0 {
		return nil, nil
	}
	_, std := stat.PopMeanStdDev(stddevpop, nil)
	return FloatValue(std), nil
}

type WINDOW_STDDEV_SAMP struct {
}

func (f *WINDOW_STDDEV_SAMP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var stddevsamp []float64
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	if len(values) < 2 {
		return nil, nil
	}
	for _, value := range values {
		f64, err := value.ToFloat64()
		if err != nil {
			return nil, err
		}
		stddevsamp = append(stddevsamp, f64)
	}
	if len(stddevsamp) == 0 {
		return nil, nil
	}
	return FloatValue(stat.StdDev(stddevsamp, nil)), nil
}

type WINDOW_STDDEV = WINDOW_STDDEV_SAMP

type WINDOW_VAR_POP struct {
}

func (f *WINDOW_VAR_POP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var varpop []float64
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	if len(values) < 2 {
		return nil, nil
	}
	for _, value := range values {
		f64, err := value.ToFloat64()
		if err != nil {
			return nil, err
		}
		varpop = append(varpop, f64)
	}
	if len(varpop) == 0 {
		return nil, nil
	}
	_, variance := stat.PopMeanVariance(varpop, nil)
	return FloatValue(variance), nil
}

type WINDOW_VAR_SAMP struct {
}

func (f *WINDOW_VAR_SAMP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var varsamp []float64
	values, err := agg.RelevantValues()
	if err != nil {
		return nil, err
	}
	if len(values) < 2 {
		return nil, nil
	}
	for _, value := range values {
		f64, err := value.ToFloat64()
		if err != nil {
			return nil, err
		}
		varsamp = append(varsamp, f64)
	}
	if len(varsamp) == 0 {
		return nil, nil
	}
	return FloatValue(stat.Variance(varsamp, nil)), nil
}

type WINDOW_VARIANCE = WINDOW_VAR_SAMP
