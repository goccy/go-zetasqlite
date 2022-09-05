package internal

import (
	"bytes"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
)

func ASCII(v string) (Value, error) {
	return IntValue(v[0]), nil
}

func BYTE_LENGTH(v []byte) (Value, error) {
	return IntValue(len(v)), nil
}

func CHAR_LENGTH(v []byte) (Value, error) {
	return IntValue(len([]rune(string(v)))), nil
}

func CHR(v int64) (Value, error) {
	return StringValue(string(rune(v))), nil
}

func CODE_POINTS_TO_BYTES(v *ArrayValue) (Value, error) {
	bytes := make([]byte, 0, len(v.values))
	for _, vv := range v.values {
		i64, err := vv.ToInt64()
		if err != nil {
			return nil, err
		}
		bytes = append(bytes, byte(i64))
	}
	return BytesValue(bytes), nil
}

func CODE_POINTS_TO_STRING(v *ArrayValue) (Value, error) {
	runes := make([]rune, 0, len(v.values))
	for _, vv := range v.values {
		if vv == nil {
			return nil, nil
		}
		i64, err := vv.ToInt64()
		if err != nil {
			return nil, err
		}
		if i64 == 0 {
			continue
		}
		runes = append(runes, rune(i64))
	}
	return StringValue(string(runes)), nil
}

// TODO: currently unsupported COLLATE function
func COLLATE(v, spec string) (Value, error) {
	return StringValue(v), nil
}

func CONCAT(args ...Value) (Value, error) {
	var ret []byte
	for _, v := range args {
		b, err := v.ToBytes()
		if err != nil {
			return nil, err
		}
		ret = append(ret, b...)
	}
	switch args[0].(type) {
	case StringValue:
		return StringValue(string(ret)), nil
	case BytesValue:
		return BytesValue(ret), nil
	}
	return nil, fmt.Errorf("CONCAT: argument type must be STRING or BYTES")
}

func FROM_BASE32(v string) (Value, error) {
	b, err := base32.StdEncoding.DecodeString(v)
	if err != nil {
		return nil, err
	}
	return BytesValue(b), nil
}

func FROM_BASE64(v string) (Value, error) {
	b, err := base64.StdEncoding.DecodeString(v)
	if err != nil {
		return nil, err
	}
	return BytesValue(b), nil
}

func FROM_HEX(v string) (Value, error) {
	if len(v)%2 != 0 {
		v = "0" + v
	}
	b, err := hex.DecodeString(v)
	if err != nil {
		return nil, err
	}
	return BytesValue(b), nil
}

var (
	defaultInitcapDelimiters = []rune{
		' ', '[', ']', '(', ')', '{', '}', '/', '|', '\\',
		'<', '>', '!', '?', '@', '"', '^', '#', '$', '&',
		'~', '_', ',', '.', ':', ';', '*', '%', '+', '-',
	}
)

func isDelim(v rune, delimiters []rune) bool {
	for _, delim := range delimiters {
		if v == delim {
			return true
		}
	}
	return false
}

func INITCAP(value string, delimiters []rune) (Value, error) {
	if delimiters == nil {
		delimiters = defaultInitcapDelimiters
	}
	src := []rune(value)
	dst := make([]rune, 0, len(src))
	for i := 0; i < len(src); i++ {
		r := src[i]
		isCurDelim := isDelim(r, delimiters)
		switch {
		case i == 0:
			// first character is upper case.
			dst = append(dst, []rune(strings.ToUpper(string([]rune{r})))...)
		case isCurDelim:
			// if current character is delimiter, add it as is.
			dst = append(dst, r)
		default:
			// if other characters, add it as lower case.
			dst = append(dst, []rune(strings.ToLower(string([]rune{r})))...)
		}
		// break if current character is last
		if i+1 == len(src) {
			continue
		}
		// if next character is delimiter, skip current character.
		if isDelim(src[i+1], delimiters) {
			continue
		}
		if isCurDelim {
			// if current character is delimiter, add next character as upper case character and skip next character.
			dst = append(dst, []rune(strings.ToUpper(string([]rune{src[i+1]})))...)
			i++
		}
	}
	return StringValue(string(dst)), nil
}

func INSTR(source, search Value, position, occurrence int64) (Value, error) {
	if position == 0 {
		return nil, fmt.Errorf("INSTR: invalid position number. position is must be large than zero value")
	}
	if occurrence <= 0 {
		return nil, fmt.Errorf("INSTR: invalid occurrence number. occurrence is must be large than zero value. but specified %d", occurrence)
	}
	pos := int(math.Abs(float64(position)))
	if _, ok := source.(StringValue); ok {
		if _, ok := search.(StringValue); !ok {
			return nil, fmt.Errorf("INSTR: source and search are must be same type")
		}
		src, err := source.ToString()
		if err != nil {
			return nil, err
		}
		search, err := search.ToString()
		if err != nil {
			return nil, err
		}
		if pos >= len(src) {
			return nil, fmt.Errorf("INSTR: invalid position number. position %d is larger than source value length %d", pos, len(src))
		}
		length := len(src)
		if position < 0 {
			src = src[:len(src)-pos+1]
		} else {
			src = src[pos-1:]
		}
		var found int64
		for i := 0; i < len(src); i++ {
			idx := strings.Index(src[i:], search)
			if idx >= 0 {
				found++
				i += idx
			}
			if found == occurrence {
				if position < 0 {
					return IntValue(length - i - 1), nil
				}
				return IntValue(pos + i), nil
			}
		}
		return IntValue(0), nil
	}
	if _, ok := source.(BytesValue); ok {
		if _, ok := search.(BytesValue); !ok {
			return nil, fmt.Errorf("INSTR: source and search are must be same type")
		}
		src, err := source.ToBytes()
		if err != nil {
			return nil, err
		}
		search, err := search.ToBytes()
		if err != nil {
			return nil, err
		}
		if pos >= len(src) {
			return nil, fmt.Errorf("INSTR: invalid position number. position %d is larger than source value length %d", pos, len(src))
		}
		length := len(src)
		if position < 0 {
			src = src[:len(src)-pos+1]
		} else {
			src = src[pos-1:]
		}
		var found int64
		for i := 0; i < len(src); i++ {
			idx := bytes.Index(src[i:], search)
			if idx >= 0 {
				found++
				i += idx
			}
			if found == occurrence {
				if position < 0 {
					return IntValue(length - i - 1), nil
				}
				return IntValue(pos + i), nil
			}
		}
		return IntValue(0), nil
	}
	return nil, fmt.Errorf("INSTR: source and search type are must be STRING or BYTES type")
}

func LEFT(v Value, length int64) (Value, error) {
	if length < 0 {
		return nil, fmt.Errorf("LEFT: unexpected length value. length must be positive number")
	}
	switch v.(type) {
	case StringValue:
		s, err := v.ToString()
		if err != nil {
			return nil, err
		}
		runes := []rune(s)
		if len(runes) <= int(length) {
			return v, nil
		}
		return StringValue(string(runes[:length])), nil
	case BytesValue:
		b, err := v.ToBytes()
		if err != nil {
			return nil, err
		}
		if len(b) <= int(length) {
			return v, nil
		}
		return BytesValue(b[:length]), nil
	}
	return nil, fmt.Errorf("LEFT: value type is must be STRING or BYTES type")
}

func LENGTH(v Value) (Value, error) {
	switch v.(type) {
	case StringValue:
		s, err := v.ToString()
		if err != nil {
			return nil, err
		}
		runes := []rune(s)
		return IntValue(len(runes)), nil
	case BytesValue:
		b, err := v.ToBytes()
		if err != nil {
			return nil, err
		}
		return IntValue(len(b)), nil
	}
	return nil, fmt.Errorf("LENGTH: value type is must be STRING or BYTES type")
}

func LOWER(v Value) (Value, error) {
	switch v.(type) {
	case StringValue:
		s, err := v.ToString()
		if err != nil {
			return nil, err
		}
		return StringValue(strings.ToLower(s)), nil
	case BytesValue:
		b, err := v.ToBytes()
		if err != nil {
			return nil, err
		}
		return BytesValue(bytes.ToLower(b)), nil
	}
	return nil, fmt.Errorf("LOWER: value type is must be STRING or BYTES type")
}

func LTRIM(v Value, cutset string) (Value, error) {
	switch v.(type) {
	case StringValue:
		s, err := v.ToString()
		if err != nil {
			return nil, err
		}
		return StringValue(strings.TrimLeft(s, cutset)), nil
	case BytesValue:
		b, err := v.ToBytes()
		if err != nil {
			return nil, err
		}
		return BytesValue(bytes.TrimLeft(b, cutset)), nil
	}
	return nil, fmt.Errorf("LTRIM: value type is must be STRING or BYTES type")
}

func TO_BASE32(v []byte) (Value, error) {
	return StringValue(base32.StdEncoding.EncodeToString(v)), nil
}

func TO_BASE64(v []byte) (Value, error) {
	return StringValue(base64.StdEncoding.EncodeToString(v)), nil
}

func TO_CODE_POINTS(v Value) (Value, error) {
	switch v.(type) {
	case StringValue:
		s, err := v.ToString()
		if err != nil {
			return nil, err
		}
		ret := &ArrayValue{}
		for _, r := range []rune(s) {
			ret.values = append(ret.values, IntValue(r))
		}
		return ret, nil
	case BytesValue:
		b, err := v.ToBytes()
		if err != nil {
			return nil, err
		}
		ret := &ArrayValue{}
		for _, bb := range b {
			ret.values = append(ret.values, IntValue(bb))
		}
		return ret, nil
	}
	return nil, fmt.Errorf("TO_CODE_POINTS: value type is must be STRING or BYTES type")
}

func TO_HEX(v []byte) (Value, error) {
	return StringValue(hex.EncodeToString(v)), nil
}

func FORMAT(format string, args ...Value) (Value, error) {
	formatted := make([]rune, 0, len(format))
	text := []rune(format)
	var argIdx int
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '%':
			i++
			if i >= len(text) {
				break
			}
			switch text[i] {
			case '%':
				formatted = append(formatted, '%', '%')
			case 'T', 't':
				if argIdx >= len(args) {
					return nil, fmt.Errorf("invalid format: %s", format)
				}
				formatted = append(formatted, []rune(args[argIdx].Format(text[i]))...)
				argIdx++
			}
		default:
			formatted = append(formatted, text[i])
		}
	}
	return StringValue(string(formatted)), nil
}
