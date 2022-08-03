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
		return nil, fmt.Errorf("currently unsupported DATE_TRUNC with DAY")
	case "ISO_WEEK":
		return nil, fmt.Errorf("currently unsupported DATE_TRUNC with ISO_WEEK")
	case "WEEK":
		return DateValue(t.AddDate(0, 0, -int(t.Weekday()))), nil
	case "MONTH":
		return DateValue(time.Time{}.AddDate(t.Year()-1, int(t.Month())-1, 0)), nil
	case "QUARTER":
		return nil, fmt.Errorf("currently unsupported DATE_TRUNC with QUARTER")
	case "YEAR":
		return DateValue(time.Time{}.AddDate(t.Year()-1, 0, 0)), nil
	case "ISO_YEAR":
		return nil, fmt.Errorf("currently unsupported DATE_TRUNC with ISO_YAER")
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

func DATE_ADD(t time.Time, v int64, part string) (Value, error) {
	switch part {
	case "DAY":
		return DateValue(t.AddDate(0, 0, int(v))), nil
	case "WEEK":
		return DateValue(t.AddDate(0, 0, int(v*7))), nil
	case "MONTH":
		return DateValue(t.AddDate(0, int(v), 0)), nil
	case "YEAR":
		return DateValue(t.AddDate(int(v), 0, 0)), nil
	}
	return nil, fmt.Errorf("unexpected part value %s", part)
}

func DATE_SUB(t time.Time, v int64, part string) (Value, error) {
	switch part {
	case "DAY":
		return DateValue(t.AddDate(0, 0, int(-v))), nil
	case "WEEK":
		return DateValue(t.AddDate(0, 0, int(-v*7))), nil
	case "MONTH":
		return DateValue(t.AddDate(0, int(-v), 0)), nil
	case "YEAR":
		return DateValue(t.AddDate(int(-v), 0, 0)), nil
	}
	return nil, fmt.Errorf("unexpected part value %s", part)
}

func PARSE_DATE(format, date string) (Value, error) {
	t, err := parseTimeFormat(format, date, FormatTypeDate)
	if err != nil {
		return nil, err
	}
	return DateValue(*t), nil
}
