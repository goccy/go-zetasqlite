package internal

func TO_JSON(v Value, stringifyWideNumbers bool) (Value, error) {
	s, err := v.ToJSON()
	if err != nil {
		return nil, err
	}
	return JsonValue(s), nil
}

func JSON_TYPE(v JsonValue) (Value, error) {
	return StringValue(v.Type()), nil
}
