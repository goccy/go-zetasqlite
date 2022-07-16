package internal

import (
	"fmt"
	"sync"
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
		if start >= len(values) || end < 0 {
			return nil
		}
		if start < 0 {
			start = 0
		}
		if end >= len(values) {
			end = len(values) - 1
		}
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
		if start >= len(values) || end < 0 {
			return nil
		}
		if start < 0 {
			start = 0
		}
		if end >= len(values) {
			end = len(values) - 1
		}
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
		if start >= len(values) || end < 0 {
			return nil
		}
		if len(values) == 0 {
			return nil
		}
		if start < 0 {
			start = 0
		}
		if end >= len(values) {
			end = len(values) - 1
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
		if start >= len(values) || end < 0 {
			return nil
		}
		if len(values) == 0 {
			return nil
		}
		if start < 0 {
			start = 0
		}
		if end >= len(values) {
			end = len(values) - 1
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
		var orderByValues []Value
		for _, value := range agg.SortedValues {
			orderByValues = append(orderByValues, value.OrderBy[len(value.OrderBy)-1])
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
		var orderByValues []Value
		for _, value := range agg.SortedValues {
			orderByValues = append(orderByValues, value.OrderBy[len(value.OrderBy)-1])
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
		rankValue = IntValue(rank)
		return nil
	}); err != nil {
		return nil, err
	}
	return rankValue, nil
}
