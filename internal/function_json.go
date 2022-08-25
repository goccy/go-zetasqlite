package internal

func TO_JSON(v Value, stringifyWideNumbers bool) (Value, error) {
	s, err := v.ToJSON()
	if err != nil {
		return nil, err
	}
	return StringValue(s), nil
}
