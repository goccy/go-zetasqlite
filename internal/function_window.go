package internal

import (
	"fmt"
	"math"
	"sync"

	"gonum.org/v1/gonum/stat"
)

type WINDOW_SUM struct {
	initialized bool
	once        sync.Once
}

func (f *WINDOW_SUM) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	if v == nil {
		return nil
	}
	f.once.Do(func() { f.initialized = true })
	return agg.Step(v, opt)
}

func (f *WINDOW_SUM) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	if !f.initialized {
		return nil, nil
	}
	var (
		sum         Value
		initialized bool
	)
	if err := agg.Done(func(values []Value, start, end int) error {
		initialized = true
		for _, value := range values[start : end+1] {
			if sum == nil {
				sum = value
			} else {
				added, err := sum.Add(value)
				if err != nil {
					return err
				}
				sum = added
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if !initialized {
		return nil, nil
	}
	return sum, nil
}

type WINDOW_COUNT_STAR struct {
}

func (f *WINDOW_COUNT_STAR) Step(opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(IntValue(1), opt)
}

func (f *WINDOW_COUNT_STAR) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var (
		count       int64
		initialized bool
	)
	if err := agg.Done(func(values []Value, start, end int) error {
		initialized = true
		count = int64(len(values[start : end+1]))
		return nil
	}); err != nil {
		return nil, err
	}
	if !initialized {
		return nil, nil
	}
	return IntValue(count), nil
}

type WINDOW_AVG struct {
}

func (f *WINDOW_AVG) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	if v == nil {
		return nil
	}
	return agg.Step(v, opt)
}

func (f *WINDOW_AVG) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var (
		sum         Value
		avg         Value
		initialized bool
	)
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) == 0 {
			return nil
		}
		initialized = true
		for _, value := range values[start : end+1] {
			if sum == nil {
				f64, err := value.ToFloat64()
				if err != nil {
					return err
				}
				sum = FloatValue(f64)
			} else {
				added, err := sum.Add(value)
				if err != nil {
					return err
				}
				sum = added
			}
		}
		ret, err := sum.Div(FloatValue(float64(len(values[start : end+1]))))
		if err != nil {
			return err
		}
		avg = ret
		return nil
	}); err != nil {
		return nil, err
	}
	if !initialized {
		return nil, nil
	}
	return avg, nil
}

type WINDOW_FIRST_VALUE struct {
}

func (f *WINDOW_FIRST_VALUE) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	if v == nil {
		return nil
	}
	return agg.Step(v, opt)
}

func (f *WINDOW_FIRST_VALUE) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var firstValue Value
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) == 0 {
			return nil
		}
		values = values[start : end+1]
		firstValue = values[0]
		return nil
	}); err != nil {
		return nil, err
	}
	return firstValue, nil
}

type WINDOW_LAST_VALUE struct {
}

func (f *WINDOW_LAST_VALUE) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	if v == nil {
		return nil
	}
	return agg.Step(v, opt)
}

func (f *WINDOW_LAST_VALUE) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var lastValue Value
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) == 0 {
			return nil
		}
		values = values[start : end+1]
		lastValue = values[len(values)-1]
		return nil
	}); err != nil {
		return nil, err
	}
	return lastValue, nil
}

type WINDOW_LAG struct {
	lagOnce      sync.Once
	offset       int64
	defaultValue Value
}

func (f *WINDOW_LAG) Step(v Value, offset int64, defaultValue Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	if v == nil {
		return nil
	}
	f.lagOnce.Do(func() {
		f.offset = offset
		f.defaultValue = defaultValue
	})
	return agg.Step(v, opt)
}

func (f *WINDOW_LAG) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var lagValue Value
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) == 0 {
			return nil
		}
		if start-int(f.offset) < 0 {
			return nil
		}
		lagValue = values[start-int(f.offset)]
		return nil
	}); err != nil {
		return nil, err
	}
	if lagValue == nil {
		return f.defaultValue, nil
	}
	return lagValue, nil
}

type WINDOW_RANK struct {
}

func (f *WINDOW_RANK) Step(opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(IntValue(1), opt)
}

func (f *WINDOW_RANK) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var rankValue Value
	if err := agg.Done(func(_ []Value, start, end int) error {
		var (
			orderByValues []Value
			isAsc         bool = true
			isAscOnce     sync.Once
		)
		for _, value := range agg.SortedValues {
			orderByValues = append(orderByValues, value.OrderBy[len(value.OrderBy)-1].Value)
			isAscOnce.Do(func() {
				isAsc = value.OrderBy[len(value.OrderBy)-1].IsAsc
			})
		}
		if start >= len(orderByValues) || end < 0 {
			return nil
		}
		if len(orderByValues) == 0 {
			return nil
		}
		if start != end {
			return fmt.Errorf("Rank must be same value of start and end")
		}
		lastIdx := start
		var (
			rank        = 0
			sameRankNum = 1
			maxValue    int64
		)
		if isAsc {
			for idx := 0; idx <= lastIdx; idx++ {
				curValue, err := orderByValues[idx].ToInt64()
				if err != nil {
					return err
				}
				if maxValue < curValue {
					maxValue = curValue
					rank += sameRankNum
					sameRankNum = 1
				} else {
					sameRankNum++
				}
			}
		} else {
			maxValue = math.MaxInt64
			for idx := 0; idx <= lastIdx; idx++ {
				curValue, err := orderByValues[idx].ToInt64()
				if err != nil {
					return err
				}
				if maxValue > curValue {
					maxValue = curValue
					rank += sameRankNum
					sameRankNum = 1
				} else {
					sameRankNum++
				}
			}
		}
		rankValue = IntValue(rank)
		return nil
	}); err != nil {
		return nil, err
	}
	return rankValue, nil
}

type WINDOW_DENSE_RANK struct {
}

func (f *WINDOW_DENSE_RANK) Step(opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(IntValue(1), opt)
}

func (f *WINDOW_DENSE_RANK) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var rankValue Value
	if err := agg.Done(func(_ []Value, start, end int) error {
		var (
			orderByValues []Value
			isAscOnce     sync.Once
			isAsc         bool = true
		)
		for _, value := range agg.SortedValues {
			orderByValues = append(orderByValues, value.OrderBy[len(value.OrderBy)-1].Value)
			isAscOnce.Do(func() {
				isAsc = value.OrderBy[len(value.OrderBy)-1].IsAsc
			})
		}
		if start >= len(orderByValues) || end < 0 {
			return nil
		}
		if len(orderByValues) == 0 {
			return nil
		}
		if start != end {
			return fmt.Errorf("Rank must be same value of start and end")
		}
		lastIdx := start
		var (
			rank     = 0
			maxValue int64
		)
		if isAsc {
			for idx := 0; idx <= lastIdx; idx++ {
				curValue, err := orderByValues[idx].ToInt64()
				if err != nil {
					return err
				}
				if maxValue < curValue {
					maxValue = curValue
					rank++
				}
			}
		} else {
			maxValue = math.MaxInt64
			for idx := 0; idx <= lastIdx; idx++ {
				curValue, err := orderByValues[idx].ToInt64()
				if err != nil {
					return err
				}
				if maxValue > curValue {
					maxValue = curValue
					rank++
				}
			}
		}
		rankValue = IntValue(rank)
		return nil
	}); err != nil {
		return nil, err
	}
	return rankValue, nil
}

type WINDOW_ROW_NUMBER struct {
}

func (f *WINDOW_ROW_NUMBER) Step(opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	return agg.Step(IntValue(1), opt)
}

func (f *WINDOW_ROW_NUMBER) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var rowNum Value
	if err := agg.Done(func(_ []Value, start, end int) error {
		rowNum = IntValue(start + 1)
		return nil
	}); err != nil {
		return nil, err
	}
	return rowNum, nil
}

type WINDOW_CORR struct {
}

func (f *WINDOW_CORR) Step(x, y Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	if x == nil || y == nil {
		return nil
	}
	return agg.Step(&ArrayValue{values: []Value{x, y}}, opt)
}

func (f *WINDOW_CORR) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var (
		x []float64
		y []float64
	)
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) < 2 {
			return nil
		}
		for _, value := range values[start : end+1] {
			arr, err := value.ToArray()
			if err != nil {
				return err
			}
			if len(arr.values) != 2 {
				return fmt.Errorf("invalid corr arguments")
			}
			x1, err := arr.values[0].ToFloat64()
			if err != nil {
				return err
			}
			x2, err := arr.values[1].ToFloat64()
			if err != nil {
				return err
			}
			x = append(x, x1)
			y = append(y, x2)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(x) == 0 || len(y) == 0 {
		return nil, nil
	}
	return FloatValue(stat.Correlation(x, y, nil)), nil
}

type WINDOW_COVAR_POP struct {
}

func (f *WINDOW_COVAR_POP) Step(x, y Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	if x == nil || y == nil {
		return nil
	}
	return agg.Step(&ArrayValue{values: []Value{x, y}}, opt)
}

func (f *WINDOW_COVAR_POP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var (
		x []float64
		y []float64
	)
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) < 2 {
			return nil
		}
		for _, value := range values[start : end+1] {
			arr, err := value.ToArray()
			if err != nil {
				return err
			}
			if len(arr.values) != 2 {
				return fmt.Errorf("invalid corr arguments")
			}
			x1, err := arr.values[0].ToFloat64()
			if err != nil {
				return err
			}
			x2, err := arr.values[1].ToFloat64()
			if err != nil {
				return err
			}
			x = append(x, x1)
			y = append(y, x2)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(x) == 0 || len(y) == 0 {
		return nil, nil
	}
	return FloatValue(stat.Covariance(x, y, nil)), nil
}

type WINDOW_COVAR_SAMP struct {
}

func (f *WINDOW_COVAR_SAMP) Step(x, y Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	if x == nil || y == nil {
		return nil
	}
	return agg.Step(&ArrayValue{values: []Value{x, y}}, opt)
}

func (f *WINDOW_COVAR_SAMP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var (
		x []float64
		y []float64
	)
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) < 2 {
			return nil
		}
		for _, value := range values[start : end+1] {
			arr, err := value.ToArray()
			if err != nil {
				return err
			}
			if len(arr.values) != 2 {
				return fmt.Errorf("invalid corr arguments")
			}
			x1, err := arr.values[0].ToFloat64()
			if err != nil {
				return err
			}
			x2, err := arr.values[1].ToFloat64()
			if err != nil {
				return err
			}
			x = append(x, x1)
			y = append(y, x2)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(x) == 0 || len(y) == 0 {
		return nil, nil
	}
	return FloatValue(stat.Covariance(x, y, nil)), nil
}

type WINDOW_STDDEV_POP struct {
}

func (f *WINDOW_STDDEV_POP) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	if v == nil {
		return nil
	}
	return agg.Step(v, opt)
}

func (f *WINDOW_STDDEV_POP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var stddevpop []float64
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) < 2 {
			return nil
		}
		for _, value := range values[start : end+1] {
			f64, err := value.ToFloat64()
			if err != nil {
				return err
			}
			stddevpop = append(stddevpop, f64)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(stddevpop) == 0 {
		return nil, nil
	}
	_, std := stat.PopMeanStdDev(stddevpop, nil)
	return FloatValue(std), nil
}

type WINDOW_STDDEV_SAMP struct {
}

func (f *WINDOW_STDDEV_SAMP) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	if v == nil {
		return nil
	}
	return agg.Step(v, opt)
}

func (f *WINDOW_STDDEV_SAMP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var stddevsamp []float64
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) < 2 {
			return nil
		}
		for _, value := range values[start : end+1] {
			f64, err := value.ToFloat64()
			if err != nil {
				return err
			}
			stddevsamp = append(stddevsamp, f64)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(stddevsamp) == 0 {
		return nil, nil
	}
	return FloatValue(stat.StdDev(stddevsamp, nil)), nil
}

type WINDOW_VAR_POP struct {
}

func (f *WINDOW_VAR_POP) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	if v == nil {
		return nil
	}
	return agg.Step(v, opt)
}

func (f *WINDOW_VAR_POP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var varpop []float64
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) < 2 {
			return nil
		}
		for _, value := range values[start : end+1] {
			f64, err := value.ToFloat64()
			if err != nil {
				return err
			}
			varpop = append(varpop, f64)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(varpop) == 0 {
		return nil, nil
	}
	_, variance := stat.PopMeanVariance(varpop, nil)
	return FloatValue(variance), nil
}

type WINDOW_VAR_SAMP struct {
}

func (f *WINDOW_VAR_SAMP) Step(v Value, opt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
	if v == nil {
		return nil
	}
	return agg.Step(v, opt)
}

func (f *WINDOW_VAR_SAMP) Done(agg *WindowFuncAggregatedStatus) (Value, error) {
	var varsamp []float64
	if err := agg.Done(func(values []Value, start, end int) error {
		if len(values) < 2 {
			return nil
		}
		for _, value := range values[start : end+1] {
			f64, err := value.ToFloat64()
			if err != nil {
				return err
			}
			varsamp = append(varsamp, f64)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(varsamp) == 0 {
		return nil, nil
	}
	return FloatValue(stat.Variance(varsamp, nil)), nil
}
