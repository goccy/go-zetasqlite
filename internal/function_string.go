package internal

import (
	"bytes"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/text/unicode/norm"
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

func ENDS_WITH(value, ends Value) (Value, error) {
	switch value.(type) {
	case StringValue:
		s, err := value.ToString()
		if err != nil {
			return nil, err
		}
		e, err := ends.ToString()
		if err != nil {
			return nil, err
		}
		return BoolValue(strings.HasSuffix(s, e)), nil
	case BytesValue:
		b, err := value.ToBytes()
		if err != nil {
			return nil, err
		}
		e, err := ends.ToBytes()
		if err != nil {
			return nil, err
		}
		return BoolValue(bytes.HasSuffix(b, e)), nil
	}
	return nil, fmt.Errorf("ENDS_WITH: argument type must be STRING or BYTES")
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

func LPAD(originalValue Value, returnLength int64, pattern Value) (Value, error) {
	switch originalValue.(type) {
	case StringValue:
		s, err := originalValue.ToString()
		if err != nil {
			return nil, err
		}
		runes := []rune(s)
		if len(runes) >= int(returnLength) {
			return StringValue(string(runes[:returnLength])), nil
		}
		remainLen := int(returnLength) - len(runes)
		var pat []rune
		if pattern == nil {
			pat = []rune(strings.Repeat(" ", remainLen))
		} else {
			p, err := pattern.ToString()
			if err != nil {
				return nil, err
			}
			pat = []rune(p)
			if remainLen-len(pat) > 0 {
				// needs to repeat pattern
				repeatNum := ((remainLen - len(pat)) / len(pat)) + 2
				pat = []rune(strings.Repeat(string(pat), repeatNum))
			}
		}
		return StringValue(string(pat[:remainLen]) + s), nil
	case BytesValue:
		b, err := originalValue.ToBytes()
		if err != nil {
			return nil, err
		}
		if len(b) >= int(returnLength) {
			return BytesValue(b[:returnLength]), nil
		}
		remainLen := int(returnLength) - len(b)
		var pat []byte
		if pattern == nil {
			pat = bytes.Repeat([]byte{' '}, remainLen)
		} else {
			p, err := pattern.ToBytes()
			if err != nil {
				return nil, err
			}
			if remainLen-len(p) > 0 {
				// needs to repeat pattern
				repeatNum := ((remainLen - len(p)) / len(p)) + 2
				pat = bytes.Repeat(p, repeatNum)
			}
		}
		return BytesValue(append(pat[:remainLen], b...)), nil
	}
	return nil, fmt.Errorf("LPAD: original value type is must be STRING or BYTES type")
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

func NORMALIZE(v, mode string) (Value, error) {
	switch mode {
	case "NFC":
		return StringValue(norm.NFC.String(v)), nil
	case "NFD":
		return StringValue(norm.NFD.String(v)), nil
	case "NFKC":
		return StringValue(norm.NFKC.String(v)), nil
	case "NFKD":
		return StringValue(norm.NFKD.String(v)), nil
	}
	return nil, fmt.Errorf("unexpected normalize mode %s", mode)
}

func NORMALIZE_AND_CASEFOLD(v, mode string) (Value, error) {
	v = strings.ToLower(v)
	switch mode {
	case "NFC":
		return StringValue(norm.NFC.String(v)), nil
	case "NFD":
		return StringValue(norm.NFD.String(v)), nil
	case "NFKC":
		return StringValue(norm.NFKC.String(v)), nil
	case "NFKD":
		return StringValue(norm.NFKD.String(v)), nil
	}
	return nil, fmt.Errorf("unexpected normalize mode %s", mode)
}

func compileRegexp(expr string) (*regexp.Regexp, error) {
	// if regexp literal has escape characters, it must be unescaped before compile.
	e, err := strconv.Unquote(`"` + expr + `"`)
	if err != nil {
		e = expr
	}
	return regexp.Compile(e)
}

func REGEXP_CONTAINS(value, expr string) (Value, error) {
	re, err := compileRegexp(expr)
	if err != nil {
		return nil, err
	}
	return BoolValue(re.MatchString(value)), nil
}

func REGEXP_EXTRACT(value Value, expr string, position, occurrence int64) (Value, error) {
	if position <= 0 {
		return nil, fmt.Errorf("REGEXP_EXTRACT: unexpected position number. position must be positive number")
	}
	if occurrence <= 0 {
		return nil, fmt.Errorf("REGEXP_EXTRACT: unexpected occurrence number. occurrence must be positive number")
	}
	re, err := compileRegexp(expr)
	if err != nil {
		return nil, err
	}
	pos := int(position) - 1
	switch value.(type) {
	case StringValue:
		v, err := value.ToString()
		if err != nil {
			return nil, err
		}
		if pos >= len([]rune(v)) {
			return nil, nil
		}
		matches := re.FindAllStringSubmatch(v[pos:], int(occurrence))
		if len(matches) < int(occurrence) {
			return nil, nil
		}
		match := matches[occurrence-1]
		return StringValue(match[len(match)-1]), nil
	case BytesValue:
		v, err := value.ToBytes()
		if err != nil {
			return nil, err
		}
		if pos >= len(v) {
			return nil, nil
		}
		matches := re.FindAllSubmatch(v[pos:], int(occurrence))
		if len(matches) < int(occurrence) {
			return nil, nil
		}
		match := matches[occurrence-1]
		return BytesValue(match[len(match)-1]), nil
	}
	return nil, fmt.Errorf("REGEXP_EXTRACT: value argument must be STRING or BYTES")
}

func REGEXP_EXTRACT_ALL(value Value, expr string) (Value, error) {
	re, err := compileRegexp(expr)
	if err != nil {
		return nil, err
	}
	switch value.(type) {
	case StringValue:
		v, err := value.ToString()
		if err != nil {
			return nil, err
		}
		matches := re.FindAllStringSubmatch(v, -1)
		ret := &ArrayValue{}
		for _, match := range matches {
			ret.values = append(ret.values, StringValue(match[len(match)-1]))
		}
		return ret, nil
	case BytesValue:
		v, err := value.ToBytes()
		if err != nil {
			return nil, err
		}
		matches := re.FindAllSubmatch(v, -1)
		ret := &ArrayValue{}
		for _, match := range matches {
			ret.values = append(ret.values, BytesValue(match[len(match)-1]))
		}
		return ret, nil
	}
	return nil, fmt.Errorf("REGEXP_EXTRACT_ALL: value argument must be STRING or BYTES")
}

func REGEXP_INSTR(sourceValue, exprValue Value, position, occurrence, occurrencePos int64) (Value, error) {
	if position <= 0 {
		return nil, fmt.Errorf("REGEXP_INSTR: unexpected position number. position must be positive number")
	}
	if occurrence <= 0 {
		return nil, fmt.Errorf("REGEXP_INSTR: unexpected occurrence number. occurrence must be positive number")
	}
	pos := int(position) - 1
	switch sourceValue.(type) {
	case StringValue:
		source, err := sourceValue.ToString()
		if err != nil {
			return nil, err
		}
		expr, err := exprValue.ToString()
		if err != nil {
			return nil, err
		}
		re, err := compileRegexp(expr)
		if err != nil {
			return nil, err
		}
		if pos >= len([]rune(source)) {
			return IntValue(0), nil
		}
		matches := re.FindAllStringSubmatchIndex(source[pos:], int(occurrence))
		if len(matches) < int(occurrence) {
			return IntValue(0), nil
		}
		match := matches[occurrence-1]
		if len(match) <= int(occurrencePos) {
			return IntValue(0), nil
		}
		return IntValue(pos + match[occurrencePos] + 1), nil
	case BytesValue:
		source, err := sourceValue.ToBytes()
		if err != nil {
			return nil, err
		}
		expr, err := exprValue.ToBytes()
		if err != nil {
			return nil, err
		}
		re, err := compileRegexp(string(expr))
		if err != nil {
			return nil, err
		}
		if pos >= len(source) {
			return IntValue(0), nil
		}
		matches := re.FindAllSubmatchIndex(source[pos:], int(occurrence))
		if len(matches) < int(occurrence) {
			return IntValue(0), nil
		}
		match := matches[occurrence-1]
		if len(match) <= int(occurrencePos) {
			return IntValue(0), nil
		}
		return IntValue(pos + match[occurrencePos] + 1), nil
	}
	return nil, fmt.Errorf("REGEXP_INSTR: source value must be STRING or BYTES")
}

func STARTS_WITH(value, starts Value) (Value, error) {
	switch value.(type) {
	case StringValue:
		v, err := value.ToString()
		if err != nil {
			return nil, err
		}
		s, err := starts.ToString()
		if err != nil {
			return nil, err
		}
		return BoolValue(strings.HasPrefix(v, s)), nil
	case BytesValue:
		v, err := value.ToBytes()
		if err != nil {
			return nil, err
		}
		s, err := starts.ToBytes()
		if err != nil {
			return nil, err
		}
		return BoolValue(bytes.HasPrefix(v, s)), nil
	}
	return nil, fmt.Errorf("ENDS_WITH: argument type must be STRING or BYTES")
}

func STRPOS(value, search Value) (Value, error) {
	switch value.(type) {
	case StringValue:
		v, err := value.ToString()
		if err != nil {
			return nil, err
		}
		s, err := search.ToString()
		if err != nil {
			return nil, err
		}
		return IntValue(strings.Index(v, s) + 1), nil
	case BytesValue:
		v, err := value.ToBytes()
		if err != nil {
			return nil, err
		}
		s, err := search.ToBytes()
		if err != nil {
			return nil, err
		}
		return IntValue(bytes.Index(v, s) + 1), nil
	}
	return nil, fmt.Errorf("STRPOS: argument type must be STRING or BYTES")
}

func substrPos(pos int64, strlen int64) int64 {
	if pos == 0 || pos < -strlen {
		return 0
	}
	if pos > strlen {
		return strlen
	}
	if pos > 0 {
		return pos - 1
	}
	// pos is negative number
	return strlen + pos
}

func substrLen(length *int64, strlen int64) (int64, error) {
	if length == nil {
		return strlen, nil
	}
	if *length < 0 {
		return 0, fmt.Errorf("SUBSTR: length must be positive number")
	}
	if *length > strlen {
		return strlen, nil
	}
	return *length, nil
}

func SUBSTR(value Value, pos int64, length *int64) (Value, error) {
	switch value.(type) {
	case StringValue:
		v, err := value.ToString()
		if err != nil {
			return nil, err
		}
		runes := []rune(v)
		runesLen := int64(len(runes))
		actualPos := substrPos(pos, runesLen)
		actualLen, err := substrLen(length, runesLen)
		if err != nil {
			return nil, err
		}
		startIdx := actualPos
		endIdx := actualPos + actualLen
		if endIdx > runesLen {
			endIdx = runesLen
		}
		return StringValue(v[startIdx:endIdx]), nil
	case BytesValue:
		v, err := value.ToBytes()
		if err != nil {
			return nil, err
		}
		vLen := int64(len(v))
		actualPos := substrPos(pos, vLen)
		actualLen, err := substrLen(length, vLen)
		if err != nil {
			return nil, err
		}
		startIdx := actualPos
		endIdx := actualPos + actualLen
		if endIdx > vLen {
			endIdx = vLen
		}
		return BytesValue(v[startIdx:endIdx]), nil
	}
	return nil, fmt.Errorf("STRPOS: argument type must be STRING or BYTES")
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

func TRANSLATE(expr, source, target Value) (Value, error) {
	switch expr.(type) {
	case StringValue:
		if _, ok := source.(StringValue); !ok {
			return nil, fmt.Errorf("TRANSLATE: source characters must be STRING type")
		}
		if _, ok := target.(StringValue); !ok {
			return nil, fmt.Errorf("TRANSLATE: target characters must be STRING type")
		}
		e, err := expr.ToString()
		if err != nil {
			return nil, err
		}
		s, err := source.ToString()
		if err != nil {
			return nil, err
		}
		t, err := target.ToString()
		if err != nil {
			return nil, err
		}
		evaluatedByte := map[byte]struct{}{}
		for i := 0; i < len(s); i++ {
			if _, exists := evaluatedByte[s[i]]; exists {
				return nil, fmt.Errorf("TRANSLATE: found duplicated source character: %c", s[i])
			}
			if len(t) > i {
				e = strings.ReplaceAll(e, string(s[i]), string(t[i]))
			} else {
				e = strings.ReplaceAll(e, string(s[i]), "")
			}
			evaluatedByte[s[i]] = struct{}{}
		}
		return StringValue(e), nil
	case BytesValue:
		if _, ok := source.(BytesValue); !ok {
			return nil, fmt.Errorf("TRANSLATE: source characters must be BYTES type")
		}
		if _, ok := target.(BytesValue); !ok {
			return nil, fmt.Errorf("TRANSLATE: target characters must be BYTES type")
		}
		e, err := expr.ToBytes()
		if err != nil {
			return nil, err
		}
		s, err := source.ToBytes()
		if err != nil {
			return nil, err
		}
		t, err := target.ToBytes()
		if err != nil {
			return nil, err
		}
		evaluatedByte := map[byte]struct{}{}
		for i := 0; i < len(s); i++ {
			if _, exists := evaluatedByte[s[i]]; exists {
				return nil, fmt.Errorf("TRANSLATE: found duplicated source character: %c", s[i])
			}
			if len(t) > i {
				e = bytes.ReplaceAll(e, []byte{s[i]}, []byte{t[i]})
			} else {
				e = bytes.ReplaceAll(e, []byte{s[i]}, []byte{})
			}
			evaluatedByte[s[i]] = struct{}{}
		}
		return BytesValue(e), nil
	}
	return nil, fmt.Errorf("TRANSLATE: expression type is must be STRING or BYTES type")
}

func TRIM(v, cutsetV Value) (Value, error) {
	var cutset string
	if cutsetV == nil {
		cutset = " "
	} else {
		b, err := cutsetV.ToBytes()
		if err != nil {
			return nil, err
		}
		cutset = string(b)
	}
	switch v.(type) {
	case StringValue:
		s, err := v.ToString()
		if err != nil {
			return nil, err
		}
		return StringValue(strings.Trim(s, cutset)), nil
	case BytesValue:
		b, err := v.ToBytes()
		if err != nil {
			return nil, err
		}
		return BytesValue(bytes.Trim(b, cutset)), nil
	}
	return nil, fmt.Errorf("TRIM: expression type is must be STRING or BYTES type")
}

func UNICODE(v string) (Value, error) {
	runes := []rune(v)
	if len(runes) == 0 {
		return IntValue(0), nil
	}
	return IntValue(runes[0]), nil
}

func UPPER(v Value) (Value, error) {
	switch v.(type) {
	case StringValue:
		s, err := v.ToString()
		if err != nil {
			return nil, err
		}
		return StringValue(strings.ToUpper(s)), nil
	case BytesValue:
		b, err := v.ToBytes()
		if err != nil {
			return nil, err
		}
		return BytesValue(bytes.ToUpper(b)), nil
	}
	return nil, fmt.Errorf("UPPER: value type is must be STRING or BYTES type")
}
