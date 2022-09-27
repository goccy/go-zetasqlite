package internal

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/goccy/go-json"
	"github.com/goccy/go-zetasql/types"
)

type ValueEncoder struct {
}

func (e *ValueEncoder) EncodeFromValue(v Value) (string, error) {
	if v == nil {
		return "null", nil
	}
	format, err := v.ToValueFormat()
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(format)
	if err != nil {
		return "", fmt.Errorf("failed to encode value: %w", err)
	}
	encoded := base64.StdEncoding.EncodeToString(b)
	return fmt.Sprintf(`"%s"`, encoded), nil
}

func (e *ValueEncoder) EncodeFromZetaSQLValue(v types.Value) (string, error) {
	value, err := e.ValueFromZetaSQLValue(v)
	if err != nil {
		return "", err
	}
	return e.EncodeFromValue(value)
}

func (e *ValueEncoder) EncodeFromGoValue(t types.Type, v interface{}) (string, error) {
	value, err := e.ValueFromGoValue(v)
	if err != nil {
		return "", err
	}
	casted, err := e.CastValue(t, value)
	if err != nil {
		return "", err
	}
	return e.EncodeFromValue(casted)
}

func (e *ValueEncoder) ValueFromZetaSQLValue(v types.Value) (Value, error) {
	if v.IsNull() {
		return nil, nil
	}
	switch v.Type().Kind() {
	case types.INT32, types.INT64, types.UINT32, types.UINT64, types.ENUM:
		return e.intValueFromLiteral(v.SQLLiteral(0))
	case types.BOOL:
		return e.boolValueFromLiteral(v.SQLLiteral(0))
	case types.FLOAT, types.DOUBLE:
		return e.floatValueFromLiteral(v.SQLLiteral(0))
	case types.STRING:
		return e.stringValueFromLiteral(v.SQLLiteral(0))
	case types.BYTES:
		return e.bytesValueFromLiteral(v.SQLLiteral(0))
	case types.DATE:
		return e.dateValueFromLiteral(v.ToInt64())
	case types.DATETIME:
		return e.datetimeValueFromLiteral(v.ToPacked64DatetimeMicros())
	case types.TIME:
		return e.timeValueFromLiteral(v.ToPacked64TimeMicros())
	case types.TIMESTAMP:
		return e.timestampValueFromLiteral(v.ToTime())
	case types.NUMERIC, types.BIG_NUMERIC:
		return e.numericValueFromLiteral(v.SQLLiteral(0))
	case types.INTERVAL:
		return e.intervalValueFromLiteral(v.SQLLiteral(0))
	case types.JSON:
		return e.jsonValueFromLiteral(v.JSONString())
	case types.ARRAY:
		return e.arrayValueFromLiteral(v)
	case types.STRUCT:
		return e.structValueFromLiteral(v)
	}
	return nil, fmt.Errorf("unsupported literal type: %s", v.Type().Kind())
}

func (e *ValueEncoder) intValueFromLiteral(lit string) (IntValue, error) {
	v, err := strconv.ParseInt(lit, 10, 64)
	if err != nil {
		return 0, err
	}
	return IntValue(v), nil
}

func (e *ValueEncoder) boolValueFromLiteral(lit string) (BoolValue, error) {
	v, err := strconv.ParseBool(lit)
	if err != nil {
		return false, err
	}
	return BoolValue(v), nil
}

func (e *ValueEncoder) floatValueFromLiteral(lit string) (FloatValue, error) {
	v, err := strconv.ParseFloat(lit, 64)
	if err != nil {
		return 0, err
	}
	return FloatValue(v), nil
}

func (e *ValueEncoder) stringValueFromLiteral(lit string) (StringValue, error) {
	return StringValue(lit), nil
}

func (e *ValueEncoder) bytesValueFromLiteral(lit string) (BytesValue, error) {
	// use a workaround because ToBytes doesn't work with certain values.
	unquoted, err := strconv.Unquote(lit[1:])
	if err != nil {
		return BytesValue(lit), nil
	}
	return BytesValue(unquoted), nil
}

func (e *ValueEncoder) dateValueFromLiteral(days int64) (DateValue, error) {
	t := time.Unix(int64(time.Duration(days)*24*time.Hour/time.Second), 0)
	return DateValue(t), nil
}

const (
	microSecondShift = 20
	secShift         = 0
	minShift         = 6
	hourShift        = 12
	dayShift         = 17
	monthShift       = 22
	yearShift        = 26
	secMask          = 0b111111
	minMask          = 0b111111 << minShift
	hourMask         = 0b11111 << hourShift
	dayMask          = 0b11111 << dayShift
	monthMask        = 0b1111 << monthShift
	yearMask         = 0x3FFF << yearShift
)

func (e *ValueEncoder) datetimeValueFromLiteral(bit int64) (DatetimeValue, error) {
	b := bit >> 20
	year := (b & yearMask) >> yearShift
	month := (b & monthMask) >> monthShift
	day := (b & dayMask) >> dayShift
	hour := (b & hourMask) >> hourShift
	min := (b & minMask) >> minShift
	sec := (b & secMask) >> secShift
	t := time.Date(
		int(year),
		time.Month(month),
		int(day),
		int(hour),
		int(min),
		int(sec),
		0, time.UTC,
	)
	return DatetimeValue(t), nil
}

func (e *ValueEncoder) timeValueFromLiteral(bit int64) (TimeValue, error) {
	b := bit >> 20
	hour := (b & hourMask) >> hourShift
	min := (b & minMask) >> minShift
	sec := (b & secMask) >> secShift
	t := time.Date(0, 0, 0, int(hour), int(min), int(sec), 0, time.UTC)
	return TimeValue(t), nil
}

func (e *ValueEncoder) timestampValueFromLiteral(t time.Time) (TimestampValue, error) {
	return TimestampValue(t), nil
}

func (e *ValueEncoder) numericValueFromLiteral(lit string) (*NumericValue, error) {
	r := new(big.Rat)
	r.SetString(lit)
	return (*NumericValue)(r), nil
}

func (e *ValueEncoder) jsonValueFromLiteral(lit string) (JsonValue, error) {
	return JsonValue(lit), nil
}

func (e *ValueEncoder) intervalValueFromLiteral(lit string) (IntervalValue, error) {
	return "", fmt.Errorf("currently unsupported INTERVAL literal")
}

func (e *ValueEncoder) arrayValueFromLiteral(v types.Value) (*ArrayValue, error) {
	ret := &ArrayValue{}
	for i := 0; i < v.NumElements(); i++ {
		elem := v.Element(i)
		value, err := e.ValueFromZetaSQLValue(elem)
		if err != nil {
			return nil, err
		}
		ret.values = append(ret.values, value)
	}
	return ret, nil
}

func (e *ValueEncoder) structValueFromLiteral(v types.Value) (*StructValue, error) {
	ret := &StructValue{
		m: map[string]Value{},
	}
	structType := v.Type().AsStruct()
	for i := 0; i < v.NumFields(); i++ {
		field := v.Field(i)
		name := structType.Field(i).Name()
		value, err := e.ValueFromZetaSQLValue(field)
		if err != nil {
			return nil, err
		}
		ret.keys = append(ret.keys, name)
		ret.values = append(ret.values, value)
		ret.m[name] = value
	}
	return ret, nil
}

func (e *ValueEncoder) CastValue(t types.Type, v Value) (Value, error) {
	if v == nil {
		return nil, nil
	}
	switch t.Kind() {
	case types.INT32, types.INT64, types.UINT32, types.UINT64, types.ENUM:
		i64, err := v.ToInt64()
		if err != nil {
			return nil, err
		}
		return IntValue(i64), nil
	case types.BOOL:
		b, err := v.ToBool()
		if err != nil {
			return nil, err
		}
		return BoolValue(b), nil
	case types.FLOAT, types.DOUBLE:
		f64, err := v.ToFloat64()
		if err != nil {
			return nil, err
		}
		return FloatValue(f64), nil
	case types.STRING:
		s, err := v.ToString()
		if err != nil {
			return nil, err
		}
		return StringValue(s), nil
	case types.BYTES:
		b, err := v.ToBytes()
		if err != nil {
			return nil, err
		}
		return BytesValue(b), nil
	case types.DATE:
		t, err := v.ToTime()
		if err != nil {
			return nil, err
		}
		return DateValue(t), nil
	case types.DATETIME:
		t, err := v.ToTime()
		if err != nil {
			return nil, err
		}
		return DatetimeValue(t), nil
	case types.TIME:
		t, err := v.ToTime()
		if err != nil {
			return nil, err
		}
		return TimeValue(t), nil
	case types.TIMESTAMP:
		t, err := v.ToTime()
		if err != nil {
			return nil, err
		}
		return TimestampValue(t), nil
	case types.INTERVAL:
		return nil, fmt.Errorf("currently unsupported interval value: %v", v)
	case types.ARRAY:
		array, err := v.ToArray()
		if err != nil {
			return nil, err
		}
		elemType := t.AsArray().ElementType()
		ret := &ArrayValue{}
		for _, value := range array.values {
			casted, err := e.CastValue(elemType, value)
			if err != nil {
				return nil, err
			}
			ret.values = append(ret.values, casted)
		}
		return ret, nil
	case types.STRUCT:
		s, err := v.ToStruct()
		if err != nil {
			return nil, err
		}
		typ := t.AsStruct()
		if typ.NumFields() != len(s.m) {
			return nil, fmt.Errorf(
				"unexpected field number. struct type expected field number %d but got %d",
				typ.NumFields(),
				len(s.m),
			)
		}
		ret := &StructValue{m: s.m}
		for i := 0; i < typ.NumFields(); i++ {
			key := typ.Field(i).Name()
			value, exists := s.m[key]
			if !exists {
				return nil, fmt.Errorf("failed to find struct field value: %s", key)
			}
			casted, err := e.CastValue(typ.Field(i).Type(), value)
			if err != nil {
				return nil, err
			}
			ret.keys = append(ret.keys, key)
			ret.values = append(ret.values, casted)
			ret.m[key] = casted
		}
		return ret, nil
	case types.NUMERIC, types.BIG_NUMERIC:
		r, err := v.ToRat()
		if err != nil {
			return nil, err
		}
		return (*NumericValue)(r), nil
	case types.JSON:
		j, err := v.ToJSON()
		if err != nil {
			return nil, err
		}
		return JsonValue(j), nil
	}
	return nil, fmt.Errorf("unsupported cast %s value", t.Kind())
}

func (e *ValueEncoder) ValueFromGoValue(v interface{}) (Value, error) {
	if v == nil {
		return nil, nil
	}
	rv := reflect.ValueOf(v)
	if _, ok := v.([]byte); ok {
		if rv.IsNil() {
			return nil, nil
		}
	}
	return e.ValueFromGoReflectValue(rv)
}

func (e *ValueEncoder) ValueFromGoReflectValue(v reflect.Value) (Value, error) {
	kind := v.Type().Kind()
	switch kind {
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
		return IntValue(v.Int()), nil
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return IntValue(int64(v.Uint())), nil
	case reflect.Float32, reflect.Float64:
		return FloatValue(v.Float()), nil
	case reflect.Bool:
		return BoolValue(v.Bool()), nil
	case reflect.String:
		return StringValue(v.String()), nil
	case reflect.Slice, reflect.Array:
		ret := &ArrayValue{}
		for i := 0; i < v.Len(); i++ {
			elem, err := e.ValueFromGoReflectValue(v.Index(i))
			if err != nil {
				return nil, err
			}
			ret.values = append(ret.values, elem)
		}
		return ret, nil
	case reflect.Map:
		ret := &StructValue{m: map[string]Value{}}
		iter := v.MapRange()
		for iter.Next() {
			key, err := e.ValueFromGoReflectValue(iter.Key())
			if err != nil {
				return nil, err
			}
			k, err := key.ToString()
			if err != nil {
				return nil, err
			}
			value, err := e.ValueFromGoReflectValue(iter.Value())
			if err != nil {
				return nil, err
			}
			ret.keys = append(ret.keys, k)
			ret.values = append(ret.values, value)
			ret.m[k] = value
		}
		return ret, nil
	case reflect.Struct:
		t, ok := v.Interface().(time.Time)
		if ok {
			return TimestampValue(t), nil
		}
		ret := &StructValue{m: map[string]Value{}}
		typ := v.Type()
		for i := 0; i < v.NumField(); i++ {
			key := typ.Field(i).Name
			value, err := e.ValueFromGoReflectValue(v.Field(i))
			if err != nil {
				return nil, err
			}
			ret.keys = append(ret.keys, key)
			ret.values = append(ret.values, value)
			ret.m[key] = value
		}
		return ret, nil
	case reflect.Ptr:
		return e.ValueFromGoReflectValue(v.Elem())
	}
	return nil, fmt.Errorf("cannot convert %s type to zetasqlite value type", kind)
}
