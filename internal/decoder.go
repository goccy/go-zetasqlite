package internal

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"reflect"
	"time"

	"github.com/goccy/go-json"
)

func DecodeValue(v interface{}) (Value, error) {
	if v == nil {
		return nil, nil
	}
	rv := reflect.ValueOf(v)
	if _, ok := v.([]byte); ok {
		if rv.IsNil() {
			return nil, nil
		}
	}
	switch vv := v.(type) {
	case int64:
		return IntValue(vv), nil
	case float64:
		return FloatValue(vv), nil
	case bool:
		return BoolValue(vv), nil
	}
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected value type: %T", v)
	}
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("failed to decode value: %w", err)
	}
	var format ValueFormat
	if err := json.Unmarshal(decoded, &format); err != nil {
		return nil, fmt.Errorf("failed to get value format: %w", err)
	}
	return DecodeFromValueFormat(&format)
}

func DecodeFromValueFormat(format *ValueFormat) (Value, error) {
	switch format.Header {
	case StringValueType:
		return StringValue(format.Body), nil
	case BytesValueType:
		decoded, err := base64.StdEncoding.DecodeString(format.Body)
		if err != nil {
			return nil, err
		}
		return BytesValue(decoded), nil
	case NumericValueType:
		r := new(big.Rat)
		r.SetString(format.Body)
		return (*NumericValue)(r), nil
	case DateValueType:
		t, err := parseDate(format.Body)
		if err != nil {
			return nil, err
		}
		return DateValue(t), nil
	case DatetimeValueType:
		t, err := parseDatetime(format.Body)
		if err != nil {
			return nil, err
		}
		return DatetimeValue(t), nil
	case TimeValueType:
		t, err := parseTime(format.Body)
		if err != nil {
			return nil, err
		}
		return TimeValue(t), nil
	case TimestampValueType:
		t, err := parseTimestamp(format.Body, time.UTC)
		if err != nil {
			return nil, err
		}
		return TimestampValue(t), nil
	case IntervalValueType:
		return nil, fmt.Errorf("failed to decode interval value")
	case JsonValueType:
		return JsonValue(format.Body), nil
	case ArrayValueType:
		var arr []interface{}
		if err := json.Unmarshal([]byte(format.Body), &arr); err != nil {
			return nil, fmt.Errorf("failed to decode array body: %w", err)
		}
		ret := &ArrayValue{
			values: make([]Value, 0, len(arr)),
		}
		for _, elem := range arr {
			value, err := DecodeValue(elem)
			if err != nil {
				return nil, err
			}
			ret.values = append(ret.values, value)
		}
		return ret, nil
	case StructValueType:
		var codec StructValueCodec
		if err := json.Unmarshal([]byte(format.Body), &codec); err != nil {
			return nil, err
		}
		m := map[string]Value{}
		values := make([]Value, 0, len(codec.Values))
		for i, data := range codec.Values {
			value, err := DecodeValue(data)
			if err != nil {
				return nil, err
			}
			m[codec.Keys[i]] = value
			values = append(values, value)
		}
		ret := &StructValue{}
		ret.keys = codec.Keys
		ret.values = values
		ret.m = m
		return ret, nil
	}
	return nil, fmt.Errorf("unexpected value header: %s", format.Header)
}
