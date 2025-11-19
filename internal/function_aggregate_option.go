package internal

import (
	"github.com/goccy/go-json"
)

type AggregatorFuncOption struct {
	Type  AggregatorFuncOptionType `json:"type"`
	Value interface{}              `json:"value"`
}

func (o *AggregatorFuncOption) UnmarshalJSON(b []byte) error {
	type aggregatorFuncOption AggregatorFuncOption

	var v aggregatorFuncOption
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	o.Type = v.Type
	switch v.Type {
	case AggregatorFuncOptionDistinct:
	case AggregatorFuncOptionIgnoreNulls:
	case AggregatorFuncOptionLimit:
		var value struct {
			Value int64 `json:"value"`
		}
		if err := json.Unmarshal(b, &value); err != nil {
			return err
		}
		o.Value = value.Value
	case AggregatorFuncOptionOrderBy:
		var value struct {
			Value *AggregateOrderBy `json:"value"`
		}
		if err := json.Unmarshal(b, &value); err != nil {
			return err
		}
		o.Value = value.Value
	case AggregatorFuncOptionHavingMax, AggregatorFuncOptionHavingMin:
		var value struct {
			Value interface{} `json:"value"`
		}
		if err := json.Unmarshal(b, &value); err != nil {
			return err
		}
		havingValue, err := ValueFromGoValue(value.Value)
		if err != nil {
			return err
		}
		o.Value = havingValue
	}
	return nil
}

type AggregatorFuncOptionType string

const (
	AggregatorFuncOptionUnknown     AggregatorFuncOptionType = "aggregate_unknown"
	AggregatorFuncOptionDistinct    AggregatorFuncOptionType = "aggregate_distinct"
	AggregatorFuncOptionLimit       AggregatorFuncOptionType = "aggregate_limit"
	AggregatorFuncOptionOrderBy     AggregatorFuncOptionType = "aggregate_order_by"
	AggregatorFuncOptionIgnoreNulls AggregatorFuncOptionType = "aggregate_ignore_nulls"
	AggregatorFuncOptionHavingMax   AggregatorFuncOptionType = "aggregate_having_max"
	AggregatorFuncOptionHavingMin   AggregatorFuncOptionType = "aggregate_having_min"
)

func DISTINCT() (Value, error) {
	b, _ := json.Marshal(&AggregatorFuncOption{
		Type: AggregatorFuncOptionDistinct,
	})
	return StringValue(string(b)), nil
}

func LIMIT(limit int64) (Value, error) {
	b, _ := json.Marshal(&AggregatorFuncOption{
		Type:  AggregatorFuncOptionLimit,
		Value: limit,
	})
	return StringValue(string(b)), nil
}

func IGNORE_NULLS() (Value, error) {
	b, _ := json.Marshal(&AggregatorFuncOption{
		Type: AggregatorFuncOptionIgnoreNulls,
	})
	return StringValue(string(b)), nil
}

type AggregateOrderBy struct {
	Value Value `json:"value"`
	IsAsc bool  `json:"isAsc"`
}

func (a *AggregateOrderBy) UnmarshalJSON(b []byte) error {
	var v struct {
		Value interface{} `json:"value"`
		IsAsc bool        `json:"isAsc"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	value, err := ValueFromGoValue(v.Value)
	if err != nil {
		return err
	}
	a.Value = value
	a.IsAsc = v.IsAsc
	return nil
}

func ORDER_BY(value Value, isAsc bool) (Value, error) {
	b, _ := json.Marshal(&AggregatorFuncOption{
		Type: AggregatorFuncOptionOrderBy,
		Value: &AggregateOrderBy{
			Value: value,
			IsAsc: isAsc,
		},
	})
	return StringValue(string(b)), nil
}

func HAVING_MAX(value Value) (Value, error) {
	b, _ := json.Marshal(&AggregatorFuncOption{
		Type:  AggregatorFuncOptionHavingMax,
		Value: value,
	})
	return StringValue(string(b)), nil
}

func HAVING_MIN(value Value) (Value, error) {
	b, _ := json.Marshal(&AggregatorFuncOption{
		Type:  AggregatorFuncOptionHavingMin,
		Value: value,
	})
	return StringValue(string(b)), nil
}

type AggregateHavingModifier struct {
	Kind  string // "MAX" or "MIN"
	Value Value
}

type AggregatorOption struct {
	Distinct       bool
	IgnoreNulls    bool
	Limit          *int64
	OrderBy        []*AggregateOrderBy
	HavingModifier *AggregateHavingModifier
}

func parseAggregateOptions(args ...Value) ([]Value, *AggregatorOption, error) {
	var (
		filteredArgs []Value
		opt          = &AggregatorOption{}
	)
	for _, arg := range args {
		if arg == nil {
			filteredArgs = append(filteredArgs, nil)
			continue
		}
		text, err := arg.ToString()
		if err != nil {
			filteredArgs = append(filteredArgs, arg)
			continue
		}
		var v AggregatorFuncOption
		if err := json.Unmarshal([]byte(text), &v); err != nil {
			filteredArgs = append(filteredArgs, arg)
			continue
		}
		switch v.Type {
		case AggregatorFuncOptionDistinct:
			opt.Distinct = true
		case AggregatorFuncOptionIgnoreNulls:
			opt.IgnoreNulls = true
		case AggregatorFuncOptionLimit:
			i64 := v.Value.(int64)
			opt.Limit = &i64
		case AggregatorFuncOptionOrderBy:
			opt.OrderBy = append(opt.OrderBy, v.Value.(*AggregateOrderBy))
		case AggregatorFuncOptionHavingMax:
			opt.HavingModifier = &AggregateHavingModifier{
				Kind:  "MAX",
				Value: v.Value.(Value),
			}
		case AggregatorFuncOptionHavingMin:
			opt.HavingModifier = &AggregateHavingModifier{
				Kind:  "MIN",
				Value: v.Value.(Value),
			}
		default:
			filteredArgs = append(filteredArgs, arg)
			continue
		}
	}
	return filteredArgs, opt, nil
}
