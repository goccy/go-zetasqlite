package internal

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/goccy/go-json"
)

type ValueDecoder struct {
}

func (d *ValueDecoder) Decode(v string) (Value, error) {
	if len(v) > 0 && v[0] == '"' {
		unquoted, err := strconv.Unquote(v)
		if err != nil {
			return nil, fmt.Errorf("failed to unquote for value: %w", err)
		}
		v = unquoted
	}
	decoded, err := base64.StdEncoding.DecodeString(v)
	if err != nil {
		return nil, fmt.Errorf("failed to decode value: %w", err)
	}
	var format ValueFormat
	if err := json.Unmarshal(decoded, &format); err != nil {
		return nil, fmt.Errorf("failed to get value format: %w", err)
	}
	return d.DecodeFromValueFormat(&format)
}

func (d *ValueDecoder) DecodeFromValueFormat(format *ValueFormat) (Value, error) {
	switch format.Header {
	case IntValueType:
		i64, err := strconv.ParseInt(format.Body, 10, 64)
		if err != nil {
			return nil, err
		}
		return IntValue(i64), nil
	case StringValueType:
		decoded, err := base64.StdEncoding.DecodeString(format.Body)
		if err != nil {
			return nil, err
		}
		return StringValue(decoded), nil
	case BytesValueType:
		decoded, err := base64.StdEncoding.DecodeString(format.Body)
		if err != nil {
			return nil, err
		}
		return BytesValue(decoded), nil
	case FloatValueType:
		f64, err := strconv.ParseFloat(format.Body, 64)
		if err != nil {
			return nil, err
		}
		return FloatValue(f64), nil
	case NumericValueType:
		r := new(big.Rat)
		r.SetString(format.Body)
		return (*NumericValue)(r), nil
	case BoolValueType:
		b, err := strconv.ParseBool(format.Body)
		if err != nil {
			return nil, err
		}
		return BoolValue(b), nil
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
		var arr []*ValueFormat
		if err := json.Unmarshal([]byte(format.Body), &arr); err != nil {
			return nil, err
		}
		ret := &ArrayValue{
			values: make([]Value, 0, len(arr)),
		}
		for _, elem := range arr {
			value, err := d.DecodeFromValueFormat(elem)
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
		for i, format := range codec.Values {
			value, err := d.DecodeFromValueFormat(format)
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
