package internal

func PARSE_DATETIME(format, date string) (Value, error) {
	t, err := parseTimeFormat(format, date, FormatTypeDatetime)
	if err != nil {
		return nil, err
	}
	return DatetimeValue(*t), nil
}
