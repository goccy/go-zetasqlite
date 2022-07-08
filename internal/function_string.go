package internal

import (
	"fmt"
)

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
