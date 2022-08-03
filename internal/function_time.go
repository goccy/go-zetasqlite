package internal

func PARSE_TIME(format, date string) (Value, error) {
	t, err := parseTimeFormat(format, date, FormatTypeTime)
	if err != nil {
		return nil, err
	}
	return TimeValue(*t), nil
}
