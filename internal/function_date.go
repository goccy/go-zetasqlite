package internal

import (
	"fmt"
	"time"
)

func CURRENT_DATE() (Value, error) {
	return CURRENT_DATE_WITH_TIME(time.Now())
}

func CURRENT_DATE_WITH_TIME(v time.Time) (Value, error) {
	return DateValue(v), nil
}

func CURRENT_DATETIME() (Value, error) {
	return CURRENT_DATETIME_WITH_TIME(time.Now())
}

func CURRENT_DATETIME_WITH_TIME(v time.Time) (Value, error) {
	return DatetimeValue(v), nil
}

func CURRENT_TIME() (Value, error) {
	return CURRENT_TIME_WITH_TIME(time.Now())
}

func CURRENT_TIME_WITH_TIME(v time.Time) (Value, error) {
	return TimeValue(v), nil
}

func CURRENT_TIMESTAMP() (Value, error) {
	return CURRENT_TIMESTAMP_WITH_TIME(time.Now())
}

func CURRENT_TIMESTAMP_WITH_TIME(v time.Time) (Value, error) {
	return TimestampValue(v), nil
}

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
	} else if len(args) == 2 {
		t, err := args[0].ToTime()
		if err != nil {
			return nil, err
		}
		return DateValue(t), nil
	} else {
		t, err := args[0].ToTime()
		if err != nil {
			return nil, err
		}
		return DateValue(t), nil
	}
	return nil, fmt.Errorf("DATE: unsupported arguments type %v", args)
}

func DATE_TRUNC(a Value, part string) (Value, error) {
	t, err := a.ToTime()
	if err != nil {
		return nil, err
	}
	switch part {
	case "DAY":
		return nil, fmt.Errorf("currently unsupported DATE_TRUN with DAY")
	case "WEEK":
		return nil, fmt.Errorf("currently unsupported DATE_TRUN with WEEK")
	case "MONTH":
		return DateValue(t.AddDate(0, 0, t.Day()-1)), nil
	case "YEAR":
		return nil, fmt.Errorf("currently unsupported DATE_TRUN with YEAR")
	}
	return nil, fmt.Errorf("unexpected part value %s", part)
}

func DATE_DIFF(a, b Value, part string) (Value, error) {
	va, err := a.ToTime()
	if err != nil {
		return nil, err
	}
	vb, err := b.ToTime()
	if err != nil {
		return nil, err
	}
	switch part {
	case "DAY":
		return IntValue(va.Day() - vb.Day()), nil
	case "MONTH":
		return IntValue(va.Month() - vb.Month()), nil
	case "YEAR":
		return IntValue(va.Year() - vb.Year()), nil
	}
	return nil, fmt.Errorf("unexpected part value %s", part)
}

func DATE_ADD(a, b Value) (Value, error) {
	return a.Add(b)
}

func DATE_SUB(a, b Value) (Value, error) {
	return a.Sub(b)
}
