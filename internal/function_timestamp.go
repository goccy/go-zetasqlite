package internal

func PARSE_TIMESTAMP(format, date string) (Value, error) {
	t, err := parseTimeFormat(format, date, FormatTypeTimestamp)
	if err != nil {
		return nil, err
	}
	return TimestampValue(*t), nil
}
