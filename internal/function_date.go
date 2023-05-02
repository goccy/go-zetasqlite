package internal

import (
	"fmt"
	"time"
)

func CURRENT_DATE(zone string) (Value, error) {
	loc, err := toLocation(zone)
	if err != nil {
		return nil, err
	}
	return CURRENT_DATE_WITH_TIME(time.Now().In(loc))
}

func CURRENT_DATE_WITH_TIME(v time.Time) (Value, error) {
	return DateValue(v), nil
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
		zone, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		loc, err := toLocation(zone)
		if err != nil {
			return nil, err
		}
		return DateValue(t.In(loc)), nil
	} else {
		t, err := args[0].ToTime()
		if err != nil {
			return nil, err
		}
		return DateValue(t), nil
	}
}

func DATE_ADD(t time.Time, v int64, part string) (Value, error) {
	switch part {
	case "DAY":
		return DateValue(t.AddDate(0, 0, int(v))), nil
	case "WEEK":
		return DateValue(t.AddDate(0, 0, int(v*7))), nil
	case "MONTH":
		return DateValue(addMonth(t, int(v))), nil
	case "YEAR":
		return DateValue(addYear(t, int(v))), nil
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
		return DateValue(addMonth(t, int(-v))), nil
	case "YEAR":
		return DateValue(addYear(t, int(-v))), nil
	}
	return nil, fmt.Errorf("unexpected part value %s", part)
}

func DATE_DIFF(a, b time.Time, part string) (Value, error) {
	switch part {
	case "DAY":
		return IntValue(a.Day() - b.Day()), nil
	case "WEEK":
		_, aWeek := a.ISOWeek()
		_, bWeek := b.ISOWeek()
		return IntValue(aWeek - bWeek), nil
	case "MONTH":
		return IntValue(a.Month() - b.Month()), nil
	case "YEAR":
		return IntValue(a.Year() - b.Year()), nil
	}
	return nil, fmt.Errorf("unexpected part value %s", part)
}

func DATE_TRUNC(t time.Time, part string) (Value, error) {
	switch part {
	case "DAY":
		return DateValue(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())), nil
	case "ISOWEEK":
		return nil, fmt.Errorf("currently unsupported DATE_TRUNC with ISO_WEEK")
	case "WEEK":
		return DateValue(t.AddDate(0, 0, -int(t.Weekday()))), nil
	case "MONTH":
		return DateValue(time.Time{}.AddDate(t.Year()-1, int(t.Month())-1, 0)), nil
	case "QUARTER":
		return nil, fmt.Errorf("currently unsupported DATE_TRUNC with QUARTER")
	case "YEAR":
		return DateValue(time.Time{}.AddDate(t.Year()-1, 0, 0)), nil
	case "ISOYEAR":
		return nil, fmt.Errorf("currently unsupported DATE_TRUNC with ISO_YAER")
	}
	return nil, fmt.Errorf("unexpected part value %s", part)
}

func DATE_FROM_UNIX_DATE(unixdate int64) (Value, error) {
	t := time.Unix(int64(time.Duration(unixdate)*24*time.Hour/time.Second), 0)
	return DateValue(t), nil
}

func FORMAT_DATE(format string, t time.Time) (Value, error) {
	s, err := formatTime(format, &t, FormatTypeDate)
	if err != nil {
		return nil, err
	}
	return StringValue(s), nil
}

func LAST_DAY(t time.Time, part string) (Value, error) {
	switch part {
	case "YEAR":
		return DateValue(time.Date(t.Year()+1, time.Month(1), 0, 0, 0, 0, 0, t.Location())), nil
	case "QUARTER":
		return nil, fmt.Errorf("LAST_DAY: unimplemented QUARTER part")
	case "MONTH":
		return DateValue(t.AddDate(0, 1, -t.Day())), nil
	case "WEEK":
		return DateValue(t.AddDate(0, 0, 6-int(t.Weekday()))), nil
	case "WEEK_MONDAY":
		return DateValue(t.AddDate(0, 0, 7-int(t.Weekday()))), nil
	case "WEEK_TUESDAY":
		return DateValue(t.AddDate(0, 0, 8-int(t.Weekday()))), nil
	case "WEEK_WEDNESDAY":
		return DateValue(t.AddDate(0, 0, 9-int(t.Weekday()))), nil
	case "WEEK_THURSDAY":
		return DateValue(t.AddDate(0, 0, 10-int(t.Weekday()))), nil
	case "WEEK_FRIDAY":
		return DateValue(t.AddDate(0, 0, 11-int(t.Weekday()))), nil
	case "WEEK_SATURDAY":
		return DateValue(t.AddDate(0, 0, 12-int(t.Weekday()))), nil
	case "ISOWEEK":
		return DateValue(t.AddDate(0, 0, 6-int(t.Weekday()))), nil
	case "ISOYEAR":
		return DateValue(time.Date(t.Year()+1, time.Month(1), 0, 0, 0, 0, 0, t.Location())), nil
	}
	return nil, fmt.Errorf("LAST_DAY: unexpected part %s", part)
}

func PARSE_DATE(format, date string) (Value, error) {
	t, err := parseTimeFormat(format, date, FormatTypeDate)
	if err != nil {
		return nil, err
	}
	return DateValue(*t), nil
}

func UNIX_DATE(t time.Time) (Value, error) {
	return IntValue(t.Unix() / int64(24*time.Hour/time.Second)), nil
}

func addMonth(t time.Time, m int) time.Time {
	curYear, curMonth, curDay := t.Date()

	first := time.Date(curYear, curMonth, 1, 0, 0, 0, 0, t.Location())
	year, month, _ := first.AddDate(0, m, 0).Date()
	after := time.Date(year, month, curDay, 0, 0, 0, 0, time.UTC)
	if month != after.Month() {
		return first.AddDate(0, m+1, -1)
	}
	return t.AddDate(0, m, 0)
}

func addYear(t time.Time, y int) time.Time {
	curYear, curMonth, curDay := t.Date()

	first := time.Date(curYear, curMonth, 1, 0, 0, 0, 0, t.Location())
	year, month, _ := first.AddDate(y, 0, 0).Date()
	after := time.Date(year, month, curDay, 0, 0, 0, 0, t.Location())
	if month != after.Month() {
		return first.AddDate(y, 1, -1)
	}
	return t.AddDate(y, 0, 0)
}
