package internal

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type OrderedValue struct {
	OrderBy []*AggregateOrderBy
	Value   Value
}

type ARRAY struct {
	once   sync.Once
	opt    *AggregatorOption
	values []*OrderedValue
}

func (f *ARRAY) Step(v Value, opt *AggregatorOption) error {
	f.once.Do(func() { f.opt = opt })
	f.values = append(f.values, &OrderedValue{
		Value: v,
	})
	return nil
}

func (f *ARRAY) Done() (Value, error) {
	values := make([]Value, 0, len(f.values))
	for _, v := range f.values {
		values = append(values, v.Value)
	}
	return &ArrayValue{
		values: values,
	}, nil
}

type ARRAY_AGG struct {
	once   sync.Once
	opt    *AggregatorOption
	values []*OrderedValue
}

func (f *ARRAY_AGG) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return fmt.Errorf("ARRAY_AGG: NULL value unsupported")
	}
	f.once.Do(func() { f.opt = opt })
	f.values = append(f.values, &OrderedValue{
		OrderBy: opt.OrderBy,
		Value:   v,
	})
	return nil
}

func (f *ARRAY_AGG) Done() (Value, error) {
	if f.opt != nil && len(f.opt.OrderBy) != 0 {
		for orderBy := 0; orderBy < len(f.opt.OrderBy); orderBy++ {
			if f.opt.OrderBy[orderBy].IsAsc {
				sort.Slice(f.values, func(i, j int) bool {
					v, _ := f.values[i].OrderBy[orderBy].Value.LT(f.values[j].OrderBy[orderBy].Value)
					return v
				})
			} else {
				sort.Slice(f.values, func(i, j int) bool {
					v, _ := f.values[i].OrderBy[orderBy].Value.GT(f.values[j].OrderBy[orderBy].Value)
					return v
				})
			}
		}
	}
	if f.opt != nil && f.opt.Limit != nil {
		minLen := int64(len(f.values))
		if *f.opt.Limit < minLen {
			minLen = *f.opt.Limit
		}
		f.values = f.values[:minLen]
	}
	values := make([]Value, 0, len(f.values))
	for _, v := range f.values {
		values = append(values, v.Value)
	}
	return &ArrayValue{
		values: values,
	}, nil
}

type ARRAY_CONCAT_AGG struct {
	once   sync.Once
	opt    *AggregatorOption
	values []*OrderedValue
}

func (f *ARRAY_CONCAT_AGG) Step(v *ArrayValue, opt *AggregatorOption) error {
	if v == nil {
		return fmt.Errorf("ARRAY_CONCAT_AGG: NULL value unsupported")
	}
	f.once.Do(func() { f.opt = opt })
	for _, vv := range v.values {
		f.values = append(f.values, &OrderedValue{
			OrderBy: opt.OrderBy,
			Value:   vv,
		})
	}
	return nil
}

func (f *ARRAY_CONCAT_AGG) Done() (Value, error) {
	if f.opt != nil && len(f.opt.OrderBy) != 0 {
		for orderBy := 0; orderBy < len(f.opt.OrderBy); orderBy++ {
			if f.opt.OrderBy[orderBy].IsAsc {
				sort.Slice(f.values, func(i, j int) bool {
					v, _ := f.values[i].OrderBy[orderBy].Value.LT(f.values[j].OrderBy[orderBy].Value)
					return v
				})
			} else {
				sort.Slice(f.values, func(i, j int) bool {
					v, _ := f.values[i].OrderBy[orderBy].Value.GT(f.values[j].OrderBy[orderBy].Value)
					return v
				})
			}
		}
	}
	if f.opt != nil && f.opt.Limit != nil {
		minLen := int64(len(f.values))
		if *f.opt.Limit < minLen {
			minLen = *f.opt.Limit
		}
		f.values = f.values[:minLen]
	}
	values := make([]Value, 0, len(f.values))
	for _, v := range f.values {
		values = append(values, v.Value)
	}
	return &ArrayValue{
		values: values,
	}, nil
}

type SUM struct {
	sum Value
}

func (f *SUM) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	if f.sum == nil {
		f.sum = v
	} else {
		added, err := f.sum.Add(v)
		if err != nil {
			return err
		}
		f.sum = added
	}
	return nil
}

func (f *SUM) Done() (Value, error) {
	return f.sum, nil
}

type BIT_AND_AGG struct {
	value Value
}

func (f *BIT_AND_AGG) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	i64, err := v.ToInt64()
	if err != nil {
		return err
	}
	if f.value == nil {
		f.value = IntValue(i64)
	} else {
		curI64, err := f.value.ToInt64()
		if err != nil {
			return err
		}
		f.value = IntValue(curI64 & i64)
	}
	return nil
}

func (f *BIT_AND_AGG) Done() (Value, error) {
	return f.value, nil
}

type BIT_OR_AGG struct {
	value int64
}

func (f *BIT_OR_AGG) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	i64, err := v.ToInt64()
	if err != nil {
		return err
	}
	if f.value == -1 {
		f.value = i64
	} else {
		f.value |= i64
	}
	return nil
}

func (f *BIT_OR_AGG) Done() (Value, error) {
	return IntValue(f.value), nil
}

type BIT_XOR_AGG struct {
	value int64
}

func (f *BIT_XOR_AGG) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	i64, err := v.ToInt64()
	if err != nil {
		return err
	}
	if f.value == 1 {
		f.value = i64
	} else {
		f.value ^= i64
	}
	return nil
}

func (f *BIT_XOR_AGG) Done() (Value, error) {
	return IntValue(f.value), nil
}

type COUNT struct {
	count Value
}

func (f *COUNT) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	if f.count == nil {
		f.count = IntValue(1)
	} else {
		added, err := f.count.Add(IntValue(1))
		if err != nil {
			return err
		}
		f.count = added
	}
	return nil
}

func (f *COUNT) Done() (Value, error) {
	return f.count, nil
}

type COUNT_STAR struct {
	count int64
}

func (f *COUNT_STAR) Step(opt *AggregatorOption) error {
	f.count++
	return nil
}

func (f *COUNT_STAR) Done() (Value, error) {
	return IntValue(f.count), nil
}

type COUNTIF struct {
	count Value
}

func (f *COUNTIF) Step(cond Value, opt *AggregatorOption) error {
	if cond == nil {
		return nil
	}
	b, err := cond.ToBool()
	if err != nil {
		return err
	}
	if b {
		if f.count == nil {
			f.count = IntValue(1)
		} else {
			added, err := f.count.Add(IntValue(1))
			if err != nil {
				return err
			}
			f.count = added
		}
	}
	return nil
}

func (f *COUNTIF) Done() (Value, error) {
	return f.count, nil
}

type LOGICAL_AND struct {
	v bool
}

func (f *LOGICAL_AND) Step(cond Value, opt *AggregatorOption) error {
	b, err := cond.ToBool()
	if err != nil {
		return err
	}
	if !b {
		f.v = false
	}
	return nil
}

func (f *LOGICAL_AND) Done() (Value, error) {
	return BoolValue(f.v), nil
}

type LOGICAL_OR struct {
	v bool
}

func (f *LOGICAL_OR) Step(cond Value, opt *AggregatorOption) error {
	b, err := cond.ToBool()
	if err != nil {
		return err
	}
	if b {
		f.v = true
	}
	return nil
}

func (f *LOGICAL_OR) Done() (Value, error) {
	return BoolValue(f.v), nil
}

type AVG struct {
	sum Value
	num int64
}

func (f *AVG) Step(v Value, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	if f.sum == nil {
		f.sum = v
	} else {
		added, err := f.sum.Add(v)
		if err != nil {
			return err
		}
		f.sum = added
	}
	f.num++
	return nil
}

func (f *AVG) Done() (Value, error) {
	if f.sum == nil {
		return nil, nil
	}
	base, err := f.sum.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(base / float64(f.num)), nil
}

type STRING_AGG struct {
	values []*OrderedValue
	delim  string
	opt    *AggregatorOption
	once   sync.Once
}

func (f *STRING_AGG) Step(v Value, delim string, opt *AggregatorOption) error {
	if v == nil {
		return nil
	}
	f.once.Do(func() {
		if delim == "" {
			delim = ","
		}
		f.delim = delim
		f.opt = opt
	})
	f.values = append(f.values, &OrderedValue{
		OrderBy: opt.OrderBy,
		Value:   v,
	})
	return nil
}

func (f *STRING_AGG) Done() (Value, error) {
	if f.opt != nil && len(f.opt.OrderBy) != 0 {
		for orderBy := 0; orderBy < len(f.opt.OrderBy); orderBy++ {
			if f.opt.OrderBy[orderBy].IsAsc {
				sort.Slice(f.values, func(i, j int) bool {
					v, _ := f.values[i].OrderBy[orderBy].Value.LT(f.values[j].OrderBy[orderBy].Value)
					return v
				})
			} else {
				sort.Slice(f.values, func(i, j int) bool {
					v, _ := f.values[i].OrderBy[orderBy].Value.GT(f.values[j].OrderBy[orderBy].Value)
					return v
				})
			}
		}
	}
	if f.opt != nil && f.opt.Limit != nil {
		minLen := int64(len(f.values))
		if *f.opt.Limit < minLen {
			minLen = *f.opt.Limit
		}
		f.values = f.values[:minLen]
	}
	values := make([]string, 0, len(f.values))
	for _, v := range f.values {
		text, err := v.Value.ToString()
		if err != nil {
			return nil, err
		}
		values = append(values, text)
	}
	return ValueOf(strings.Join(values, f.delim))
}
