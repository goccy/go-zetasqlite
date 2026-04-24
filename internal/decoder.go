package internal

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
)

// jsonUnmarshalPreserveNumbers decodes JSON into v with json.Number so
// integer literals don't collapse to float64 when landing in an
// interface{} slot. The post-pass rewrites json.Number nodes to int64
// or float64 based on the source shape (presence of '.' or 'e').
func jsonUnmarshalPreserveNumbers(data []byte, v interface{}) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}

// coerceJSONNumber converts a json.Number into int64 (no decimal / exponent)
// or float64 (otherwise). Non-json.Number values pass through untouched.
func coerceJSONNumber(v interface{}) interface{} {
	n, ok := v.(json.Number)
	if !ok {
		return v
	}
	s := string(n)
	if !strings.ContainsAny(s, ".eE") {
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i
		}
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return v
}

func DecodeValue(v interface{}) (Value, error) {
	if isNullValue(v) {
		return nil, nil
	}
	v = coerceJSONNumber(v)
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
	var layout ValueLayout
	if err := json.Unmarshal(decoded, &layout); err != nil {
		return nil, fmt.Errorf("failed to get value layout: %w", err)
	}
	return decodeFromValueLayout(&layout)
}

func decodeFromValueLayout(layout *ValueLayout) (Value, error) {
	switch layout.Header {
	case StringValueType:
		return StringValue(layout.Body), nil
	case BytesValueType:
		decoded, err := base64.StdEncoding.DecodeString(layout.Body)
		if err != nil {
			return nil, err
		}
		return BytesValue(decoded), nil
	case NumericValueType:
		r := new(big.Rat)
		r.SetString(layout.Body)
		return &NumericValue{Rat: r}, nil
	case BigNumericValueType:
		r := new(big.Rat)
		r.SetString(layout.Body)
		return &NumericValue{Rat: r, isBigNumeric: true}, nil
	case DateValueType:
		t, err := parseDate(layout.Body)
		if err != nil {
			return nil, err
		}
		return DateValue(t), nil
	case DatetimeValueType:
		t, err := parseDatetime(layout.Body)
		if err != nil {
			return nil, err
		}
		return DatetimeValue(t), nil
	case TimeValueType:
		t, err := parseTime(layout.Body)
		if err != nil {
			return nil, err
		}
		return TimeValue(t), nil
	case TimestampValueType:
		microsec, err := strconv.ParseInt(layout.Body, 10, 64)
		microSecondsInSecond := int64(time.Second) / int64(time.Microsecond)
		sec := microsec / microSecondsInSecond
		remainder := microsec - (sec * microSecondsInSecond)
		if err != nil {
			return nil, fmt.Errorf("failed to parse unixmicro for timestamp value %s: %w", layout.Body, err)
		}
		return TimestampValue(time.Unix(sec, remainder*int64(time.Microsecond))), nil
	case IntervalValueType:
		return parseInterval(layout.Body)
	case JsonValueType:
		return JsonValue(layout.Body), nil
	case ArrayValueType:
		var arr []interface{}
		if err := jsonUnmarshalPreserveNumbers([]byte(layout.Body), &arr); err != nil {
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
		var structLayout StructValueLayout
		if err := jsonUnmarshalPreserveNumbers([]byte(layout.Body), &structLayout); err != nil {
			return nil, err
		}
		m := map[string]Value{}
		values := make([]Value, 0, len(structLayout.Values))
		for i, data := range structLayout.Values {
			value, err := DecodeValue(data)
			if err != nil {
				return nil, err
			}
			m[structLayout.Keys[i]] = value
			values = append(values, value)
		}
		ret := &StructValue{}
		ret.keys = structLayout.Keys
		ret.values = values
		ret.m = m
		return ret, nil
	case GeographyValueType:
		ret, err := GeographyFromWKT(layout.Body)
		if err != nil {
			return nil, fmt.Errorf("decodeFromValueLayout failed: %w", err)
		}
		return ret, nil
	}
	return nil, fmt.Errorf("unexpected value header: %s", layout.Header)
}
