package internal

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type DayOfWeek string

const (
	Sunday    DayOfWeek = "Sunday"
	Monday    DayOfWeek = "Monday"
	Tuesday   DayOfWeek = "Tuesday"
	Wednesday DayOfWeek = "Wednesday"
	Thursday  DayOfWeek = "Thursday"
	Friday    DayOfWeek = "Friday"
	Saturday  DayOfWeek = "Saturday"
)

type Month string

const (
	January   Month = "January"
	February  Month = "February"
	March     Month = "March"
	April     Month = "April"
	May       Month = "May"
	June      Month = "June"
	July      Month = "July"
	August    Month = "August"
	September Month = "September"
	October   Month = "October"
	November  Month = "November"
	December  Month = "December"
)

var (
	dayOfWeeks = []DayOfWeek{
		Sunday,
		Monday,
		Tuesday,
		Wednesday,
		Thursday,
		Friday,
		Saturday,
	}
	months = []Month{
		January,
		February,
		March,
		April,
		May,
		June,
		July,
		August,
		September,
		October,
		November,
		December,
	}
)

type TimeFormatType int

func (t TimeFormatType) String() string {
	switch t {
	case FormatTypeDate:
		return "date"
	case FormatTypeDatetime:
		return "datetime"
	case FormatTypeTime:
		return "time"
	case FormatTypeTimestamp:
		return "timestamp"
	}
	return "unknown"
}

const (
	FormatTypeDate      TimeFormatType = 0
	FormatTypeDatetime  TimeFormatType = 1
	FormatTypeTime      TimeFormatType = 2
	FormatTypeTimestamp TimeFormatType = 3
)

type FormatTimeInfo struct {
	AvailableTypes []TimeFormatType
	Matcher        func([]rune, *time.Time) (int, error)
}

func (i *FormatTimeInfo) Available(typ TimeFormatType) bool {
	for _, t := range i.AvailableTypes {
		if t == typ {
			return true
		}
	}
	return false
}

var formatPatternMap = map[rune]*FormatTimeInfo{
	'A': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: weekOfDayMatcher,
	},
	'a': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: shortWeekOfDayMatcher,
	},
	'B': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: monthMatcher,
	},
	'b': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: shortMonthMatcher,
	},
	'C': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: centuryMatcher,
	},
	'c': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: ansicMatcher,
	},
	'D': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: monthDayYearMatcher,
	},
	'd': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: dayMatcher,
	},
	'e': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: dayMatcher,
	},
	'F': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: yearMonthDayMatcher,
	},
	'G': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: yearISOMatcher,
	},
	'g': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: centuryISOMatcher,
	},
	'H': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: hourMatcher,
	},
	'h': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: shortMonthMatcher,
	},
	'I': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: hour12Matcher,
	},
	'J': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: yearISOMatcher,
	},
	'j': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: dayOfYearMatcher,
	},
	'k': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: hourMatcher,
	},
	'l': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: hour12Matcher,
	},
	'M': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: minuteMatcher,
	},
	'm': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: monthNumberMatcher,
	},
	'n': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTime, FormatTypeTimestamp,
		},
		Matcher: newLineMatcher,
	},
	'P': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: smallAMPMMatcher,
	},
	'p': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: largeAMPMMatcher,
	},
	'Q': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: quaterMatcher,
	},
	'R': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: hourMinuteMatcher,
	},
	'S': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: secondMatcher,
	},
	's': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: unixtimeSecondsMatcher,
	},
	'T': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: hourMinuteSecondMatcher,
	},
	't': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: tabMatcher,
	},
	'U': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: weekOfYearMatcher,
	},
	'u': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: weekNumberMatcher,
	},
	'V': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: weekOfYearISOMatcher,
	},
	'W': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: weekOfYearMatcher,
	},
	'w': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: weekNumberZeroBaseMatcher,
	},
	'X': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: hourMinuteSecondMatcher,
	},
	'x': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: monthDayYearMatcher,
	},
	'Y': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: yearMatcher,
	},
	'y': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Matcher: centuryMatcher,
	},
	'Z': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTimestamp,
		},
		Matcher: timeZoneMatcher,
	},
	'z': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTimestamp,
		},
		Matcher: timeZoneOffsetMatcher,
	},
	'%': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTime, FormatTypeTimestamp,
		},
		Matcher: escapeMatcher,
	},
}

func weekOfDayMatcher(text []rune, t *time.Time) (int, error) {
	for _, dayOfWeek := range dayOfWeeks {
		if len(text) < len(dayOfWeek) {
			continue
		}
		src := strings.ToLower(string(dayOfWeek))
		dst := strings.ToLower(string(text[:len(dayOfWeek)]))
		if src == dst {
			return len(dayOfWeek), nil
		}
	}
	return 0, fmt.Errorf("unexpected day of week")
}

func shortWeekOfDayMatcher(text []rune, t *time.Time) (int, error) {
	const shortLen = 3
	if len(text) < shortLen {
		return 0, fmt.Errorf("unexpected short day of week")
	}

	for _, dayOfWeek := range dayOfWeeks {
		src := strings.ToLower(string(dayOfWeek))[:shortLen]
		dst := strings.ToLower(string(text[:shortLen]))
		if src == dst {
			return shortLen, nil
		}
	}
	return 0, fmt.Errorf("unexpected short day of week")
}

func monthMatcher(text []rune, t *time.Time) (int, error) {
	for monthIdx, month := range months {
		if len(text) < len(month) {
			continue
		}
		src := strings.ToLower(string(month))
		dst := strings.ToLower(string(text[:len(month)]))
		if src == dst {
			*t = t.AddDate(0, int(monthIdx+1)-int(t.Month()), 0)
			return len(month), nil
		}
	}
	return 0, fmt.Errorf("unexpected month")
}

func shortMonthMatcher(text []rune, t *time.Time) (int, error) {
	const shortLen = 3

	if len(text) < shortLen {
		return 0, fmt.Errorf("unexpected short month")
	}
	for monthIdx, month := range months {
		src := strings.ToLower(string(month))[:shortLen]
		dst := strings.ToLower(string(text[:shortLen]))
		if src == dst {
			*t = t.AddDate(0, int(monthIdx+1)-int(t.Month()), 0)
			return shortLen, nil
		}
	}
	return 0, fmt.Errorf("unexpected short month")
}

func centuryMatcher(text []rune, t *time.Time) (int, error) {
	const centuryLen = 2
	if len(text) < centuryLen {
		return 0, fmt.Errorf("unexpected century number")
	}
	c, err := strconv.ParseInt(string(text[:centuryLen]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected century number")
	}
	if c < 0 {
		return 0, fmt.Errorf("invalid century number %d", c)
	}
	year := int(c*100 - 99)
	*t = t.AddDate(year-int(t.Year()), 0, 0)
	return centuryLen, nil
}

func ansicMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented ansic matcher")
}

func monthDayYearMatcher(text []rune, t *time.Time) (int, error) {
	fmtLen := len("00/00/00")
	if len(text) < fmtLen {
		return 0, fmt.Errorf("unexpected month/day/year format")
	}
	splitted := strings.Split(string(text[:fmtLen]), "/")
	if len(splitted) != 3 {
		return 0, fmt.Errorf("unexpected month/day/year format")
	}
	month := splitted[0]
	day := splitted[1]
	year := splitted[2]
	if len(month) != 2 || len(day) != 2 || len(year) != 2 {
		return 0, fmt.Errorf("unexpected month/day/year format")
	}
	m, err := strconv.ParseInt(month, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected month/day/year format: %w", err)
	}
	d, err := strconv.ParseInt(day, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected month/day/year format: %w", err)
	}
	y, err := strconv.ParseInt(year, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected month/day/year format: %w", err)
	}
	*t = t.AddDate(int(2000+y)-int(t.Year()), int(m)-int(t.Month()), int(d)-int(t.Day()))
	return fmtLen, nil
}

func dayMatcher(text []rune, t *time.Time) (int, error) {
	const dayLen = 2
	if len(text) < dayLen {
		return 0, fmt.Errorf("unexpected day number")
	}
	d, err := strconv.ParseInt(string(text[:dayLen]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected day number")
	}
	if d < 0 {
		return 0, fmt.Errorf("invalid day number %d", d)
	}
	*t = t.AddDate(0, 0, int(d)-int(t.Day()))
	return dayLen, nil
}

func yearMonthDayMatcher(text []rune, t *time.Time) (int, error) {
	fmtLen := len("2021-01-20")
	if len(text) < fmtLen {
		return 0, fmt.Errorf("unexpected year-month-day format")
	}
	splitted := strings.Split(string(text[:fmtLen]), "-")
	if len(splitted) != 3 {
		return 0, fmt.Errorf("unexpected year-month-day format")
	}
	year := splitted[0]
	month := splitted[1]
	day := splitted[2]
	if len(year) != 4 || len(month) != 2 || len(day) != 2 {
		return 0, fmt.Errorf("unexpected year-month-day format")
	}
	y, err := strconv.ParseInt(year, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected year-month-day format: %w", err)
	}
	m, err := strconv.ParseInt(month, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected year-month-day format: %w", err)
	}
	d, err := strconv.ParseInt(day, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected year-month-day format: %w", err)
	}
	*t = t.AddDate(int(y)-int(t.Year()), int(m)-int(t.Month()), int(d)-int(t.Day()))
	return fmtLen, nil
}

func yearISOMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented year ISO matcher")
}

func centuryISOMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented century ISO matcher")
}

func hourMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented hour matcher")
}

func hour12Matcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented hour12 matcher")
}

func dayOfYearMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented day of year matcher")
}

func minuteMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented minute matcher")
}

func monthNumberMatcher(text []rune, t *time.Time) (int, error) {
	const monthLen = 2
	if len(text) < monthLen {
		return 0, fmt.Errorf("unexpected month number")
	}
	m, err := strconv.ParseInt(string(text[:monthLen]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected month number")
	}
	if m < 0 {
		return 0, fmt.Errorf("invalid month number %d", m)
	}
	*t = t.AddDate(0, int(m)-int(t.Month()), 0)
	return monthLen, nil
}

func newLineMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented new line matcher")
}

func smallAMPMMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented am pm matcher")
}

func largeAMPMMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented AM PM matcher")
}

func quaterMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented quater matcher")
}

func hourMinuteMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented hour minute matcher")
}

func secondMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented second matcher")
}

func unixtimeSecondsMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented unixtime seconds matcher")
}

func hourMinuteSecondMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented hour minute second matcher")
}

func tabMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented tab matcher")
}

func weekOfYearMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented week of year matcher")
}

func weekNumberMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented week number matcher")
}

func weekOfYearISOMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented week of year ISO matcher")
}

func weekNumberZeroBaseMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented week number zero base matcher")
}

func yearMatcher(text []rune, t *time.Time) (int, error) {
	const yearLen = 4
	if len(text) < yearLen {
		return 0, fmt.Errorf("unexpected year number")
	}
	y, err := strconv.ParseInt(string(text[:yearLen]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected year number")
	}
	if y < 0 {
		return 0, fmt.Errorf("invalid year number %d", y)
	}
	*t = t.AddDate(int(y)-int(t.Year()), 0, 0)
	return yearLen, nil
}

func timeZoneMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented time zone matcher")
}

func timeZoneOffsetMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented time zone offset matcher")
}

func escapeMatcher(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented escape matcher")
}

func parseTimeFormat(formatStr, targetStr string, typ TimeFormatType) (*time.Time, error) {
	format := []rune(formatStr)
	target := []rune(targetStr)
	var (
		targetIdx int
		formatIdx int
	)
	ret := &time.Time{}
	for formatIdx < len(format) {
		c := format[formatIdx]
		if c == '%' {
			formatIdx++
			if formatIdx >= len(format) {
				return nil, fmt.Errorf("invalid time format")
			}
			c = format[formatIdx]
			info := formatPatternMap[c]
			if info == nil {
				return nil, fmt.Errorf("unexpected format type %%%s", c)
			}
			if !info.Available(typ) {
				return nil, fmt.Errorf("unavailable format by %s type", typ)
			}
			if targetIdx >= len(target) {
				return nil, fmt.Errorf("invalid target text")
			}
			progress, err := info.Matcher(target[targetIdx:], ret)
			if err != nil {
				return nil, err
			}
			targetIdx += progress
			formatIdx++
		} else {
			formatIdx++
			targetIdx++
		}
	}
	return ret, nil
}
