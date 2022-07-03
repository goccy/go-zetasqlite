package internal

import (
	"fmt"
	"time"
)

func DATE(args ...Value) (Value, error) {
	if len(args) == 3 {
		year, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		month, err := args[1].ToInt64()
		if err != nil {
			return nil, err
		}
		day, err := args[2].ToInt64()
		if err != nil {
			return nil, err
		}
		return DateValue(time.Time{}.AddDate(int(year)-1, int(month)-1, int(day)-1)), nil
	}
	return nil, fmt.Errorf("DATE: unsupported arguments type %v", args)
}

func DATE_ADD(a, b Value) (Value, error) {
	return a.Add(b)
}

func DATE_SUB(a, b Value) (Value, error) {
	return a.Sub(b)
}
