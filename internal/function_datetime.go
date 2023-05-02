package internal

import (
	"fmt"
	"time"
)

func CURRENT_DATETIME(zone string) (Value, error) {
	loc, err := toLocation(zone)
	if err != nil {
		return nil, err
	}
	return CURRENT_DATETIME_WITH_TIME(time.Now().In(loc))
}

func CURRENT_DATETIME_WITH_TIME(v time.Time) (Value, error) {
	return DatetimeValue(v), nil
}

func DATETIME(args ...Value) (Value, error) {
	if len(args) == 6 {
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
		hour, err := args[3].ToInt64()
		if err != nil {
			return nil, err
		}
		minute, err := args[4].ToInt64()
		if err != nil {
			return nil, err
		}
		second, err := args[5].ToInt64()
		if err != nil {
			return nil, err
		}
		location, err := toLocation("")
		if err != nil {
			return nil, err
		}
		return DatetimeValue(time.Date(
			int(year),
			time.Month(month),
			int(day),
			int(hour),
			int(minute),
			int(second),
			0,
			location,
		)), nil
	}
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("DATETIME: invalid argument num %d", len(args))
	}
	switch v := args[0].(type) {
	case DateValue:
		t, err := v.ToTime()
		if err != nil {
			return nil, err
		}
		if len(args) == 2 {
			t2, err := args[1].ToTime()
			if err != nil {
				return nil, fmt.Errorf("DATETIME: second argument must be time type: %w", err)
			}
			return DatetimeValue(time.Date(
				t.Year(),
				t.Month(),
				t.Day(),
				t2.Hour(),
				t2.Minute(),
				t2.Second(),
				t2.Nanosecond(),
				t2.Location(),
			)), nil
		}
		return DatetimeValue(t), nil
	case TimestampValue:
		t, err := v.ToTime()
		if err != nil {
			return nil, err
		}
		if len(args) == 2 {
			zone, err := args[1].ToString()
			if err != nil {
				return nil, fmt.Errorf("DATETIME: second argument must be string type: %w", err)
			}
			loc, err := toLocation(zone)
			if err != nil {
				return nil, err
			}
			return DatetimeValue(t.In(loc)), nil
		}
		return DatetimeValue(t), nil
	}
	return nil, fmt.Errorf("DATETIME: first argument must be DATE or TIMESTAMP type")
}

func DATETIME_ADD(t time.Time, v int64, part string) (Value, error) {
	switch part {
	case "MICROSECOND":
		return DatetimeValue(t.Add(time.Duration(v) * time.Microsecond)), nil
	case "MILLISECOND":
		return DatetimeValue(t.Add(time.Duration(v) * time.Millisecond)), nil
	case "SECOND":
		return DatetimeValue(t.Add(time.Duration(v) * time.Second)), nil
	case "MINUTE":
		return DatetimeValue(t.Add(time.Duration(v) * time.Minute)), nil
	case "HOUR":
		return DatetimeValue(t.Add(time.Duration(v) * time.Hour)), nil
	case "DAY":
		return DatetimeValue(t.AddDate(0, 0, int(v))), nil
	case "WEEK":
		return DatetimeValue(t.AddDate(0, 0, int(v*7))), nil
	case "MONTH":
		return DatetimeValue(addMonth(t, int(v))), nil
	case "QUARTER":
		return DatetimeValue(addMonth(t, 3*int(v))), nil
	case "YEAR":
		return DatetimeValue(addYear(t, int(v))), nil
	}
	return nil, fmt.Errorf("DATETIME_ADD: unexpected part value %s", part)
}

func DATETIME_SUB(t time.Time, v int64, part string) (Value, error) {
	switch part {
	case "MICROSECOND":
		return DatetimeValue(t.Add(-time.Duration(v) * time.Microsecond)), nil
	case "MILLISECOND":
		return DatetimeValue(t.Add(-time.Duration(v) * time.Millisecond)), nil
	case "SECOND":
		return DatetimeValue(t.Add(-time.Duration(v) * time.Second)), nil
	case "MINUTE":
		return DatetimeValue(t.Add(-time.Duration(v) * time.Minute)), nil
	case "HOUR":
		return DatetimeValue(t.Add(-time.Duration(v) * time.Hour)), nil
	case "DAY":
		return DatetimeValue(t.AddDate(0, 0, -int(v))), nil
	case "WEEK":
		return DatetimeValue(t.AddDate(0, 0, -int(v*7))), nil
	case "MONTH":
		return DatetimeValue(addMonth(t, -int(v))), nil
	case "QUARTER":
		return DatetimeValue(addMonth(t, -3*int(v))), nil
	case "YEAR":
		return DatetimeValue(addYear(t, -int(v))), nil
	}
	return nil, fmt.Errorf("DATETIME_SUB: unexpected part value %s", part)
}

func DATETIME_DIFF(a, b time.Time, part string) (Value, error) {
	diff := a.Sub(b)
	yearISOA, weekA := a.ISOWeek()
	yearISOB, weekB := b.ISOWeek()
	switch part {
	case "MICROSECOND":
		return IntValue(diff / time.Microsecond), nil
	case "MILLISECOND":
		return IntValue(diff / time.Millisecond), nil
	case "SECOND":
		return IntValue(diff / time.Second), nil
	case "MINUTE":
		return IntValue(diff / time.Minute), nil
	case "HOUR":
		return IntValue(diff / time.Hour), nil
	case "DAY":
		diffDay := diff / (24 * time.Hour)
		mod := diff % (24 * time.Hour)
		if mod > 0 {
			diffDay++
		} else if mod < 0 {
			diffDay--
		}
		return IntValue(diffDay), nil
	case "WEEK":
		if a.Weekday() > 0 {
			weekA--
		}
		if b.Weekday() > 0 {
			weekB--
		}
		return IntValue((a.Year()-b.Year())*48 + weekA - weekB), nil
	case "WEEK_MONDAY":
		if a.Weekday() > 1 {
			weekA--
		}
		if b.Weekday() > 1 {
			weekB--
		}
		return IntValue((a.Year()-b.Year())*48 + weekA - weekB), nil
	case "WEEK_TUESDAY":
		if a.Weekday() > 2 {
			weekA--
		}
		if b.Weekday() > 2 {
			weekB--
		}
		return IntValue((a.Year()-b.Year())*48 + weekA - weekB), nil
	case "WEEK_WEDNESDAY":
		if a.Weekday() > 3 {
			weekA--
		}
		if b.Weekday() > 3 {
			weekB--
		}
		return IntValue((a.Year()-b.Year())*48 + weekA - weekB), nil
	case "WEEK_THURSDAY":
		if a.Weekday() > 4 {
			weekA--
		}
		if b.Weekday() > 4 {
			weekB--
		}
		return IntValue((a.Year()-b.Year())*48 + weekA - weekB), nil
	case "WEEK_FRIDAY":
		if a.Weekday() > 5 {
			weekA--
		}
		if b.Weekday() > 5 {
			weekB--
		}
		return IntValue((a.Year()-b.Year())*48 + weekA - weekB), nil
	case "WEEK_SATURDAY":
		if a.Weekday() > 6 {
			weekA--
		}
		if b.Weekday() > 6 {
			weekB--
		}
		return IntValue((a.Year()-b.Year())*48 + weekA - weekB), nil
	case "ISOWEEK":
		return IntValue((a.Year()-b.Year())*48 + weekA - weekB), nil
	case "MONTH":
		return IntValue((a.Year()-b.Year())*12 + int(a.Month()) - int(b.Month())), nil
	case "QUARTER":
		return IntValue(a.Month()/4 - b.Month()/4), nil
	case "YEAR":
		return IntValue(a.Year() - b.Year()), nil
	case "ISOYEAR":
		return IntValue(yearISOA - yearISOB), nil
	}
	return nil, fmt.Errorf("DATETIME_DIFF: unexpected part value %s", part)
}

func DATETIME_TRUNC(t time.Time, part string) (Value, error) {
	yearISO, weekISO := t.ISOWeek()
	switch part {
	case "MICROSECOND":
		return DatetimeValue(t), nil
	case "MILLISECOND":
		sec := time.Duration(t.Second()) - time.Duration(t.Second())/time.Microsecond
		return DatetimeValue(time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			int(sec),
			0,
			t.Location(),
		)), nil
	case "SECOND":
		sec := time.Duration(t.Second()) / time.Second
		return DatetimeValue(time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			int(sec),
			0,
			t.Location(),
		)), nil
	case "MINUTE":
		return DatetimeValue(time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			0,
			0,
			t.Location(),
		)), nil
	case "HOUR":
		return DatetimeValue(time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			0,
			0,
			0,
			t.Location(),
		)), nil
	case "DAY":
		return DatetimeValue(time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			0,
			0,
			0,
			0,
			t.Location(),
		)), nil
	case "WEEK":
		return DatetimeValue(t.AddDate(0, 0, int(t.Weekday()))), nil
	case "WEEK_MONDAY":
		return DatetimeValue(t.AddDate(0, 0, int(t.Weekday())-6)), nil
	case "WEEK_TUESDAY":
		return DatetimeValue(t.AddDate(0, 0, int(t.Weekday())-5)), nil
	case "WEEK_WEDNESDAY":
		return DatetimeValue(t.AddDate(0, 0, int(t.Weekday())-4)), nil
	case "WEEK_THURSDAY":
		return DatetimeValue(t.AddDate(0, 0, int(t.Weekday())-3)), nil
	case "WEEK_FRIDAY":
		return DatetimeValue(t.AddDate(0, 0, int(t.Weekday())-2)), nil
	case "WEEK_SATURDAY":
		return DatetimeValue(t.AddDate(0, 0, int(t.Weekday())-1)), nil
	case "ISOWEEK":
		return DatetimeValue(time.Date(
			yearISO,
			0,
			7*weekISO,
			0,
			0,
			0,
			0,
			t.Location(),
		)), nil
	case "MONTH":
		return DatetimeValue(time.Date(
			t.Year(),
			t.Month(),
			0,
			0,
			0,
			0,
			0,
			t.Location(),
		)), nil
	case "QUARTER":
		return nil, fmt.Errorf("currently unsupported DATE_TRUNC with QUARTER")
	case "YEAR":
		return DatetimeValue(time.Date(
			t.Year(),
			1,
			1,
			0,
			0,
			0,
			0,
			t.Location(),
		)), nil
	case "ISOYEAR":
		firstDay := time.Date(
			yearISO,
			1,
			1,
			0,
			0,
			0,
			0,
			t.Location(),
		)
		return DatetimeValue(firstDay.AddDate(0, 0, 1-int(firstDay.Weekday()))), nil
	}
	return nil, fmt.Errorf("unexpected part value %s", part)
}

func FORMAT_DATETIME(format string, t time.Time) (Value, error) {
	s, err := formatTime(format, &t, FormatTypeDatetime)
	if err != nil {
		return nil, err
	}
	return StringValue(s), nil
}

func PARSE_DATETIME(format, date string) (Value, error) {
	t, err := parseTimeFormat(format, date, FormatTypeDatetime)
	if err != nil {
		return nil, err
	}
	return DatetimeValue(*t), nil
}
