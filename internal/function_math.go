package internal

import (
	"math"
)

func ABS(a Value) (Value, error) {
	f64, err := a.ToFloat64()
	if err != nil {
		return nil, err
	}
	return FloatValue(math.Abs(f64)), nil
}
