package internal

import "time"

func CURRENT_TIMESTAMP() (Value, error) {
	return CURRENT_TIMESTAMP_WITH_TIME(time.Now())
}

func CURRENT_TIMESTAMP_WITH_TIME(v time.Time) (Value, error) {
	return TimestampValue(v), nil
}

func PARSE_TIMESTAMP(format, date string) (Value, error) {
	t, err := parseTimeFormat(format, date, FormatTypeTimestamp)
	if err != nil {
		return nil, err
	}
	return TimestampValue(*t), nil
}
