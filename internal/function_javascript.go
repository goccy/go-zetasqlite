package internal

import (
	"fmt"
	"math/big"
	"time"

	"github.com/dop251/goja"
	googlesql "github.com/goccy/go-googlesql"
)

func EVAL_JAVASCRIPT(code string, retType *Type, argNames []string, args []Value) (Value, error) {
	vm := goja.New()
	for i := 0; i < len(args); i++ {
		var v interface{}
		if args[i] != nil {
			structV, ok := args[i].(*StructValue)
			if ok {
				v = structV.m
			} else {
				v = args[i].Interface()
			}
		}
		if err := vm.Set(argNames[i], v); err != nil {
			return nil, fmt.Errorf(
				"failed to set argument variable for %s as %v",
				argNames[i],
				args[i],
			)
		}
	}
	evalCode := fmt.Sprintf(`
function zetasqlite_javascript_func() { %s }
zetasqlite_javascript_func();
`, code)
	ret, err := vm.RunString(evalCode)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate javascript code %s: %w", code, err)
	}
	typ, err := retType.ToZetaSQLType()
	if err != nil {
		return nil, fmt.Errorf("failed to get return type: %w", err)
	}
	value, err := castJavaScriptValue(typ, ret)
	if err != nil {
		return nil, fmt.Errorf("failed to convert zetasqlite value from %v: %w", ret, err)
	}
	return value, nil
}

func castJavaScriptValue(t googlesql.Googlesql_TypeNode, v goja.Value) (Value, error) {
	if v == nil {
		return nil, nil
	}
	// Googlesql_TypeNode carries Kind directly.
	switch m1(t.Kind()) {
	case googlesql.TypeKindTypeInt32, googlesql.TypeKindTypeInt64, googlesql.TypeKindTypeUint32, googlesql.TypeKindTypeUint64:
		return IntValue(v.ToInteger()), nil
	case googlesql.TypeKindTypeBool:
		return BoolValue(v.ToBoolean()), nil
	case googlesql.TypeKindTypeFloat, googlesql.TypeKindTypeDouble:
		return FloatValue(v.ToFloat()), nil
	case googlesql.TypeKindTypeString, googlesql.TypeKindTypeEnum:
		return StringValue(v.ToString().String()), nil
	case googlesql.TypeKindTypeBytes:
		return BytesValue(v.ToString().String()), nil
	case googlesql.TypeKindTypeDate:
		t, err := parseDate(v.ToString().String())
		if err != nil {
			return nil, err
		}
		return DateValue(t), nil
	case googlesql.TypeKindTypeDatetime:
		t, err := parseDatetime(v.ToString().String())
		if err != nil {
			return nil, err
		}
		return DatetimeValue(t), nil
	case googlesql.TypeKindTypeTime:
		t, err := parseTime(v.ToString().String())
		if err != nil {
			return nil, err
		}
		return TimeValue(t), nil
	case googlesql.TypeKindTypeTimestamp:
		t, err := parseTimestamp(v.ToString().String(), time.UTC)
		if err != nil {
			return nil, err
		}
		return TimestampValue(t), nil
	case googlesql.TypeKindTypeInterval:
		return parseInterval(v.ToString().String())
	case googlesql.TypeKindTypeNumeric:
		r := new(big.Rat)
		r.SetString(v.ToNumber().String())
		return &NumericValue{Rat: r}, nil
	case googlesql.TypeKindTypeBignumeric:
		r := new(big.Rat)
		r.SetString(v.ToNumber().String())
		return &NumericValue{Rat: r}, nil
	case googlesql.TypeKindTypeJson:
		return JsonValue(v.ToString().String()), nil
	case googlesql.TypeKindTypeArray:
		// ArrayType.ElementType isn't exposed via the bridge; cast
		// each element through without type info.
		var ret ArrayValue
		for _, vv := range v.Export().([]interface{}) {
			base, err := ValueFromGoValue(vv)
			if err != nil {
				return nil, err
			}
			ret.values = append(ret.values, base)
		}
		return &ret, nil
	case googlesql.TypeKindTypeStruct:
		base, err := ValueFromGoValue(v.Export())
		if err != nil {
			return nil, err
		}
		return CastValue(t, base)
	case googlesql.TypeKindTypeGeography:
		base, err := ValueFromGoValue(v.Export())
		if err != nil {
			return nil, err
		}
		return CastValue(t, base)
	}
	return nil, fmt.Errorf("unsupported cast %v from JavaScript value", m1(t.Kind()))
}
