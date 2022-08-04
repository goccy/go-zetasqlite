package internal

import "time"

func CURRENT_TIME() (Value, error) {
	return CURRENT_TIME_WITH_TIME(time.Now())
}

func CURRENT_TIME_WITH_TIME(v time.Time) (Value, error) {
	return TimeValue(v), nil
}

func PARSE_TIME(format, date string) (Value, error) {
	t, err := parseTimeFormat(format, date, FormatTypeTime)
	if err != nil {
		return nil, err
	}
	return TimeValue(*t), nil
}
