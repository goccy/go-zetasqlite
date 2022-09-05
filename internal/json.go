package internal

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/goccy/go-zetasql/types"
)

func JSONFromZetaSQLValue(v types.Value) string {
	value := jsonFromZetaSQLValue(v)
	if value == "null" && v.Type().Kind() != types.JSON {
		return "null"
	}
	switch v.Type().Kind() {
	case types.BYTES:
		return toBytesValueFromString(value)
	case types.DATE:
		return toDateValueFromString(value)
	case types.DATETIME:
		return toDatetimeValueFromString(value)
	case types.TIME:
		return toTimeValueFromString(value)
	case types.TIMESTAMP:
		text, err := toTimestampValueFromString(value)
		if err != nil {
			log.Printf("%v", err)
		}
		return text
	case types.ARRAY:
		return toArrayValueFromJSONString(value)
	case types.STRUCT:
		return toStructValueFromJSONString(value)
	case types.JSON:
		v, _ := toJsonValueFromString(value)
		return v
	}
	return value
}

func jsonFromZetaSQLValue(v types.Value) string {
	if v.IsNull() {
		return "null"
	}
	switch v.Type().Kind() {
	case types.BYTES:
		// use a workaround because ToBytes doesn't work with certain values.
		b := v.SQLLiteral(0)
		s, _ := strconv.Unquote(b[1:])
		return s
	case types.DATE:
		return toDateValueFromInt64(v.ToInt64())
	case types.DATETIME:
		return toDatetimeValueFromInt64(v.ToPacked64DatetimeMicros())
	case types.TIME:
		return toTimeValueFromInt64(v.ToPacked64TimeMicros())
	case types.TIMESTAMP:
		return toTimestampValueFromTime(v.ToTime())
	case types.ARRAY:
		elems := []string{}
		for i := 0; i < v.NumElements(); i++ {
			elem := v.Element(i)
			elems = append(elems, JSONFromZetaSQLValue(elem))
		}
		return fmt.Sprintf("[%s]", strings.Join(elems, ","))
	case types.STRUCT:
		fields := []string{}
		structType := v.Type().AsStruct()
		for i := 0; i < v.NumFields(); i++ {
			field := v.Field(i)
			name := structType.Field(i).Name()
			val := JSONFromZetaSQLValue(field)
			fields = append(
				fields,
				fmt.Sprintf("%s:%s", strconv.Quote(name), string(val)),
			)
		}
		return fmt.Sprintf("{%s}", strings.Join(fields, ","))
	case types.JSON:
		return v.JSONString()
	default:
		vv := v.SQLLiteral(0)
		if vv == "NULL" {
			return "null"
		}
		return vv
	}
}
