package internal

import (
	"fmt"

	"github.com/dop251/goja"
)

func EVAL_JAVASCRIPT(code string, retType *Type, argNames []string, args []Value) (Value, error) {
	vm := goja.New()
	for i := 0; i < len(args); i++ {
		var v interface{}
		if args[i] != nil {
			v = args[i].Interface()
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
	value, err := ValueFromGoValue(ret)
	if err != nil {
		return nil, fmt.Errorf("failed to convert zetasqlite value from %v: %w", ret, err)
	}
	casted, err := CastValue(typ, value)
	if err != nil {
		return nil, fmt.Errorf("failed to convert from %v to %s: %w", value, retType.FormatType(), err)
	}
	return casted, nil
}
