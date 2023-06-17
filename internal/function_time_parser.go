package internal

import (
	"fmt"
	"regexp"
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
	Parse          func([]rune, *time.Time) (int, error)
	Format         func(*time.Time) ([]rune, error)
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
		Parse:  weekOfDayParser,
		Format: weekOfDayFormatter,
	},
	'a': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  shortWeekOfDayParser,
		Format: shortWeekOfDayFormatter,
	},
	'B': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  monthParser,
		Format: monthFormatter,
	},
	'b': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  shortMonthParser,
		Format: shortMonthFormatter,
	},
	'C': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  centuryParser,
		Format: centuryFormatter,
	},
	'c': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  ansicParser,
		Format: ansicFormatter,
	},
	'D': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  monthDayYearParser,
		Format: monthDayYearFormatter,
	},
	'd': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  dayParser,
		Format: dayFormatter,
	},
	'e': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  dayParser,
		Format: dayFormatter,
	},
	'F': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  yearMonthDayParser,
		Format: yearMonthDayFormatter,
	},
	'G': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  yearISOParser,
		Format: yearISOFormatter,
	},
	'g': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  centuryISOParser,
		Format: centuryISOFormatter,
	},
	'H': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  hourParser,
		Format: hourFormatter,
	},
	'h': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  shortMonthParser,
		Format: shortMonthFormatter,
	},
	'I': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  hour12Parser,
		Format: hour12Formatter,
	},
	'J': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  yearISOParser,
		Format: yearISOFormatter,
	},
	'j': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  dayOfYearParser,
		Format: dayOfYearFormatter,
	},
	'k': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  hourParser,
		Format: hourFormatter,
	},
	'l': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  hour12Parser,
		Format: hour12Formatter,
	},
	'M': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  minuteParser,
		Format: minuteFormatter,
	},
	'm': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  monthNumberParser,
		Format: monthNumberFormatter,
	},
	'n': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTime, FormatTypeTimestamp,
		},
		Parse:  newLineParser,
		Format: newLineFormatter,
	},
	'P': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  smallAMPMParser,
		Format: smallAMPMFormatter,
	},
	'p': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  largeAMPMParser,
		Format: largeAMPMFormatter,
	},
	'Q': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  quarterParser,
		Format: quarterFormatter,
	},
	'R': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  hourMinuteParser,
		Format: hourMinuteFormatter,
	},
	'S': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  secondParser,
		Format: secondFormatter,
	},
	's': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  unixtimeSecondsParser,
		Format: unixtimeSecondsFormatter,
	},
	'T': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  hourMinuteSecondParser,
		Format: hourMinuteSecondFormatter,
	},
	't': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  tabParser,
		Format: tabFormatter,
	},
	'U': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  weekOfYearParser,
		Format: weekOfYearFormatter,
	},
	'u': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  weekNumberParser,
		Format: weekNumberFormatter,
	},
	'V': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  weekOfYearISOParser,
		Format: weekOfYearISOFormatter,
	},
	'W': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  weekOfYearParser,
		Format: weekOfYearFormatter,
	},
	'w': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  weekNumberZeroBaseParser,
		Format: weekNumberZeroBaseFormatter,
	},
	'X': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTime, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  hourMinuteSecondParser,
		Format: hourMinuteSecondFormatter,
	},
	'x': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  monthDayYearParser,
		Format: monthDayYearFormatter,
	},
	'Y': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  yearParser,
		Format: yearFormatter,
	},
	'y': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTimestamp,
		},
		Parse:  centuryParser,
		Format: centuryFormatter,
	},
	'Z': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTimestamp,
		},
		Parse:  timeZoneParser,
		Format: timeZoneFormatter,
	},
	'z': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeTimestamp,
		},
		Parse:  timeZoneOffsetParser,
		Format: timeZoneOffsetFormatter,
	},
	'%': &FormatTimeInfo{
		AvailableTypes: []TimeFormatType{
			FormatTypeDate, FormatTypeDatetime, FormatTypeTime, FormatTypeTimestamp,
		},
		Parse:  escapeParser,
		Format: escapeFormatter,
	},
}

func weekOfDayParser(text []rune, t *time.Time) (int, error) {
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

func weekOfDayFormatter(t *time.Time) ([]rune, error) {
	return []rune(dayOfWeeks[int(t.Weekday())]), nil
}

func shortWeekOfDayParser(text []rune, t *time.Time) (int, error) {
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

func shortWeekOfDayFormatter(t *time.Time) ([]rune, error) {
	const shortLen = 3
	return []rune(string(dayOfWeeks[int(t.Weekday())])[:shortLen]), nil
}

func monthParser(text []rune, t *time.Time) (int, error) {
	for monthIdx, month := range months {
		if len(text) < len(month) {
			continue
		}
		src := strings.ToLower(string(month))
		dst := strings.ToLower(string(text[:len(month)]))
		if src == dst {
			*t = time.Date(
				int(t.Year()),
				time.Month(monthIdx+1),
				int(t.Day()),
				int(t.Hour()),
				int(t.Minute()),
				int(t.Second()),
				int(t.Nanosecond()),
				t.Location(),
			)
			return len(month), nil
		}
	}
	return 0, fmt.Errorf("unexpected month")
}

func monthFormatter(t *time.Time) ([]rune, error) {
	monthIdx := int(t.Month())
	return []rune(months[monthIdx-1]), nil
}

func shortMonthParser(text []rune, t *time.Time) (int, error) {
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

func shortMonthFormatter(t *time.Time) ([]rune, error) {
	const shortLen = 3
	monthIdx := int(t.Month())
	return []rune(string(months[monthIdx-1])[:shortLen]), nil
}

func centuryParser(text []rune, t *time.Time) (int, error) {
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
	*t = time.Date(
		int(c*100-99),
		t.Month(),
		int(t.Day()),
		int(t.Hour()),
		int(t.Minute()),
		int(t.Second()),
		int(t.Nanosecond()),
		t.Location(),
	)
	return centuryLen, nil
}

func centuryFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprint(t.Year())[:2]), nil
}

func ansicParser(text []rune, t *time.Time) (int, error) {
	v, err := time.Parse("Mon Jan 02 15:04:05 2006", string(text))
	if err != nil {
		return 0, err
	}
	*t = v
	return len(text), nil
}

func ansicFormatter(t *time.Time) ([]rune, error) {
	return []rune(t.Format("Mon Jan 02 15:04:05 2006")), nil
}

func monthDayYearParser(text []rune, t *time.Time) (int, error) {
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
	*t = time.Date(
		int(2000+y),
		time.Month(m),
		int(d),
		int(t.Hour()),
		int(t.Minute()),
		int(t.Second()),
		int(t.Nanosecond()),
		t.Location(),
	)
	return fmtLen, nil
}

func monthDayYearFormatter(t *time.Time) ([]rune, error) {
	year := fmt.Sprint(t.Year())
	return []rune(
		fmt.Sprintf(
			"%s/%s/%s",
			fmt.Sprintf("%02d", t.Month()),
			fmt.Sprintf("%02d", t.Day()),
			year[2:],
		),
	), nil
}

func dayParser(text []rune, t *time.Time) (int, error) {
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
	*t = time.Date(
		int(t.Year()),
		t.Month(),
		int(d),
		int(t.Hour()),
		int(t.Minute()),
		int(t.Second()),
		int(t.Nanosecond()),
		t.Location(),
	)
	return dayLen, nil
}

func dayFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprintf("%02d", t.Day())), nil
}

func yearMonthDayParser(text []rune, t *time.Time) (int, error) {
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
	*t = time.Date(
		int(y),
		time.Month(m),
		int(d),
		int(t.Hour()),
		int(t.Minute()),
		int(t.Second()),
		int(t.Nanosecond()),
		t.Location(),
	)
	return fmtLen, nil
}

func yearMonthDayFormatter(t *time.Time) ([]rune, error) {
	return []rune(t.Format("2006-01-02")), nil
}

func yearISOParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented year ISO matcher")
}

func yearISOFormatter(t *time.Time) ([]rune, error) {
	year, _ := t.ISOWeek()
	return []rune(fmt.Sprint(year)), nil
}

func centuryISOParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented century ISO matcher")
}

func centuryISOFormatter(t *time.Time) ([]rune, error) {
	year, _ := t.ISOWeek()
	return []rune(fmt.Sprint(year)), nil
}

func hourParser(text []rune, t *time.Time) (int, error) {
	const hourLen = 2
	if len(text) < hourLen {
		return 0, fmt.Errorf("unexpected hour number")
	}
	h, err := strconv.ParseInt(string(text[:hourLen]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected hour number")
	}
	if h < 0 || h > 24 {
		return 0, fmt.Errorf("invalid hour number %d", h)
	}
	*t = time.Date(
		int(t.Year()),
		t.Month(),
		int(t.Day()),
		int(h),
		int(t.Minute()),
		int(t.Second()),
		int(t.Nanosecond()),
		t.Location(),
	)
	return hourLen, nil
}

func hourFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprintf("%02d", t.Hour())), nil
}

func hour12Parser(text []rune, t *time.Time) (int, error) {
	const hourLen = 2
	if len(text) < hourLen {
		return 0, fmt.Errorf("unexpected hour number")
	}
	h, err := strconv.ParseInt(string(text[:hourLen]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected hour number")
	}
	if h < 0 || h > 12 {
		return 0, fmt.Errorf("invalid hour number %d", h)
	}
	*t = time.Date(
		int(t.Year()),
		t.Month(),
		int(t.Day()),
		int(h),
		int(t.Minute()),
		int(t.Second()),
		int(t.Nanosecond()),
		t.Location(),
	)
	return hourLen, nil
}

func hour12Formatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprintf("%02d", t.Hour())), nil
}

func dayOfYearParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented day of year matcher")
}

func dayOfYearFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprint(t.YearDay())), nil
}

func minuteParser(text []rune, t *time.Time) (int, error) {
	const minuteLen = 2
	if len(text) < minuteLen {
		return 0, fmt.Errorf("unexpected minute number")
	}
	m, err := strconv.ParseInt(string(text[:minuteLen]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected minute number")
	}
	if m < 0 || m > 59 {
		return 0, fmt.Errorf("invalid minute number %d", m)
	}
	*t = time.Date(
		int(t.Year()),
		t.Month(),
		int(t.Day()),
		int(t.Hour()),
		int(m),
		int(t.Second()),
		int(t.Nanosecond()),
		t.Location(),
	)
	return minuteLen, nil
}

func minuteFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprintf("%02d", t.Minute())), nil
}

func monthNumberParser(text []rune, t *time.Time) (int, error) {
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
	*t = time.Date(
		t.Year(),
		time.Month(m),
		int(t.Day()),
		int(t.Hour()),
		int(t.Minute()),
		int(t.Second()),
		int(t.Nanosecond()),
		t.Location(),
	)
	return monthLen, nil
}

func monthNumberFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprintf("%02d", t.Month())), nil
}

func newLineParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented new line matcher")
}

func newLineFormatter(t *time.Time) ([]rune, error) {
	return []rune("\n"), nil
}

func smallAMPMParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented am pm matcher")
}

func smallAMPMFormatter(t *time.Time) ([]rune, error) {
	if t.Hour() < 12 {
		return []rune("am"), nil
	}
	return []rune("pm"), nil
}

func largeAMPMParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented AM PM matcher")
}

func largeAMPMFormatter(t *time.Time) ([]rune, error) {
	if t.Hour() < 12 {
		return []rune("AM"), nil
	}
	return []rune("PM"), nil
}

func quarterParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented quater matcher")
}

func quarterFormatter(t *time.Time) ([]rune, error) {
	day := t.YearDay()
	switch {
	case day < 90:
		return []rune("1"), nil
	case day < 180:
		return []rune("2"), nil
	case day < 270:
		return []rune("3"), nil
	}
	return []rune("4"), nil
}

func hourMinuteParser(text []rune, t *time.Time) (int, error) {
	fmtLen := len("00:00")
	if len(text) < fmtLen {
		return 0, fmt.Errorf("unexpected hour:minute format")
	}
	splitted := strings.Split(string(text[:fmtLen]), ":")
	if len(splitted) != 2 {
		return 0, fmt.Errorf("unexpected hour:minute format")
	}
	hour := splitted[0]
	minute := splitted[1]
	if len(hour) != 2 || len(minute) != 2 {
		return 0, fmt.Errorf("unexpected hour:minute format")
	}
	h, err := strconv.ParseInt(hour, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected hour:minute format: %w", err)
	}
	m, err := strconv.ParseInt(minute, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected hour:minute format: %w", err)
	}
	*t = time.Date(
		int(t.Year()),
		t.Month(),
		int(t.Day()),
		int(h),
		int(m),
		int(t.Second()),
		int(t.Nanosecond()),
		t.Location(),
	)
	return fmtLen, nil
}

func hourMinuteFormatter(t *time.Time) ([]rune, error) {
	return []rune(t.Format("15:04")), nil
}

func secondParser(text []rune, t *time.Time) (int, error) {
	const secondLen = 2
	if len(text) < secondLen {
		return 0, fmt.Errorf("unexpected second number")
	}
	s, err := strconv.ParseInt(string(text[:secondLen]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected second number")
	}
	if s < 0 || s > 59 {
		return 0, fmt.Errorf("invalid second number %d", s)
	}
	*t = time.Date(
		int(t.Year()),
		t.Month(),
		int(t.Day()),
		int(t.Hour()),
		int(t.Minute()),
		int(s),
		int(t.Nanosecond()),
		t.Location(),
	)
	return secondLen, nil
}

func secondFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprintf("%02d", t.Second())), nil
}

func unixtimeSecondsParser(text []rune, t *time.Time) (int, error) {
	const unixtimeLen = 10
	if len(text) < unixtimeLen {
		return 0, fmt.Errorf("unexpected unixtime length")
	}
	u, err := strconv.ParseInt(string(text[:unixtimeLen]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected unixtime number")
	}
	if u < 0 {
		return 0, fmt.Errorf("invalid unixtime number %d", u)
	}
	*t = time.Unix(u, 0)
	return unixtimeLen, nil
}

func unixtimeSecondsFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprint(t.Unix())), nil
}

func hourMinuteSecondParser(text []rune, t *time.Time) (int, error) {
	fmtLen := len("00:00:00")
	if len(text) < fmtLen {
		return 0, fmt.Errorf("unexpected hour:minute:second format")
	}
	splitted := strings.Split(string(text[:fmtLen]), ":")
	if len(splitted) != 3 {
		return 0, fmt.Errorf("unexpected hour:minute:second format")
	}
	hour := splitted[0]
	minute := splitted[1]
	second := splitted[2]
	if len(hour) != 2 || len(minute) != 2 || len(second) != 2 {
		return 0, fmt.Errorf("unexpected hour:minute:second format")
	}
	h, err := strconv.ParseInt(hour, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected hour:minute:second format: %w", err)
	}
	m, err := strconv.ParseInt(minute, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected hour:minute:second format: %w", err)
	}
	s, err := strconv.ParseInt(second, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected hour:minute:second format: %w", err)
	}
	*t = time.Date(
		int(t.Year()),
		t.Month(),
		int(t.Day()),
		int(h),
		int(m),
		int(s),
		int(t.Nanosecond()),
		t.Location(),
	)
	return fmtLen, nil
}

func hourMinuteSecondFormatter(t *time.Time) ([]rune, error) {
	return []rune(t.Format("15:04:05")), nil
}

func tabParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented tab matcher")
}

func tabFormatter(t *time.Time) ([]rune, error) {
	return []rune("\t"), nil
}

func weekOfYearParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented week of year matcher")
}

func weekOfYearFormatter(t *time.Time) ([]rune, error) {
	_, week := t.ISOWeek()
	return []rune(fmt.Sprint(week)), nil
}

func weekNumberParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented week number matcher")
}

func weekNumberFormatter(t *time.Time) ([]rune, error) {
	_, week := t.ISOWeek()
	return []rune(fmt.Sprint(week)), nil
}

func weekOfYearISOParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented week of year ISO matcher")
}

func weekOfYearISOFormatter(t *time.Time) ([]rune, error) {
	_, week := t.ISOWeek()
	return []rune(fmt.Sprint(week)), nil
}

func weekNumberZeroBaseParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented week number zero base matcher")
}

func weekNumberZeroBaseFormatter(t *time.Time) ([]rune, error) {
	_, week := t.ISOWeek()
	return []rune(fmt.Sprint(week - 1)), nil
}

func yearParser(text []rune, t *time.Time) (int, error) {
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
	*t = time.Date(
		int(y),
		t.Month(),
		int(t.Day()),
		int(t.Hour()),
		int(t.Minute()),
		int(t.Second()),
		int(t.Nanosecond()),
		t.Location(),
	)
	return yearLen, nil
}

func yearFormatter(t *time.Time) ([]rune, error) {
	return []rune(fmt.Sprint(t.Year())), nil
}

func timeZoneParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented time zone matcher")
}

func timeZoneFormatter(t *time.Time) ([]rune, error) {
	name, _ := t.Zone()
	return []rune(name), nil
}

func timeZoneOffsetParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented time zone offset matcher")
}

func timeZoneOffsetFormatter(t *time.Time) ([]rune, error) {
	_, offset := t.Zone()
	return []rune(fmt.Sprint(offset)), nil
}

func escapeParser(text []rune, t *time.Time) (int, error) {
	return 0, fmt.Errorf("unimplemented escape matcher")
}

func escapeFormatter(t *time.Time) ([]rune, error) {
	return []rune("%"), nil
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
			if c == 'E' {
				formatIdx++
				if formatIdx >= len(format) {
					return nil, fmt.Errorf("invalid time format")
				}
				info, formatProgress, err := combinationPatternInfo(format[formatIdx:])
				if err != nil {
					return nil, err
				}
				if !info.Available(typ) {
					return nil, fmt.Errorf("unavailable format by %s type", typ)
				}
				if targetIdx >= len(target) {
					return nil, fmt.Errorf("invalid target text")
				}
				if formatIdx >= len(format) {
					return nil, fmt.Errorf("invalid format text")
				}
				progress, err := info.Parse(target[targetIdx:], ret)
				if err != nil {
					return nil, err
				}
				targetIdx += progress
				formatIdx += formatProgress
				continue
			}
			info := formatPatternMap[c]
			if info == nil {
				return nil, fmt.Errorf("unexpected format type %%%c", c)
			}
			if !info.Available(typ) {
				return nil, fmt.Errorf("unavailable format by %s type", typ)
			}
			if targetIdx >= len(target) {
				return nil, fmt.Errorf("invalid target text")
			}
			progress, err := info.Parse(target[targetIdx:], ret)
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
	if targetIdx != len(target) {
		return nil, fmt.Errorf("found unused format element %q", target[targetIdx:])
	}
	return ret, nil
}

func formatTime(formatStr string, t *time.Time, typ TimeFormatType) (string, error) {
	format := []rune(formatStr)
	var ret []rune
	for i := 0; i < len(format); i++ {
		c := format[i]
		if c == '%' {
			i++
			if i >= len(format) {
				return "", fmt.Errorf("invalid time format")
			}
			c = format[i]
			if c == 'E' {
				i++
				if i >= len(format) {
					return "", fmt.Errorf("invalid time format")
				}
				info, formatProgress, err := combinationPatternInfo(format[i:])
				if err != nil {
					return "", err
				}
				if !info.Available(typ) {
					return "", fmt.Errorf("unavailable format by %s type", typ)
				}
				formatted, err := info.Format(t)
				if err != nil {
					return "", err
				}
				ret = append(ret, formatted...)
				i += formatProgress
				continue
			}
			info := formatPatternMap[c]
			if info == nil {
				return "", fmt.Errorf("unexpected format type %%%c", c)
			}
			if !info.Available(typ) {
				return "", fmt.Errorf("unavailable format by %s type", typ)
			}
			formatted, err := info.Format(t)
			if err != nil {
				return "", err
			}
			ret = append(ret, formatted...)
		} else {
			ret = append(ret, c)
		}
	}
	return string(ret), nil
}

type CombinationFormatTimeInfo struct {
	AvailableTypes []TimeFormatType
	Parse          func([]rune, *time.Time) (int, error)
	Format         func(*time.Time) ([]rune, error)
}

func (i *CombinationFormatTimeInfo) Available(typ TimeFormatType) bool {
	for _, t := range i.AvailableTypes {
		if t == typ {
			return true
		}
	}
	return false
}

func combinationPatternInfo(format []rune) (*CombinationFormatTimeInfo, int, error) {
	switch format[0] {
	case 'z':
		return &CombinationFormatTimeInfo{
			AvailableTypes: []TimeFormatType{
				FormatTypeTimestamp,
			},
			Parse:  timeZoneRFC3339Parser,
			Format: timeZoneRFC3339Formatter,
		}, 1, nil
	case '1', '2', '3', '5', '6':
		if len(format) > 1 && format[1] == 'S' {
			precision, _ := strconv.Atoi(string(format[0]))
			return &CombinationFormatTimeInfo{
				AvailableTypes: []TimeFormatType{
					FormatTypeTime,
					FormatTypeDatetime,
					FormatTypeTimestamp,
				},
				Parse: func(target []rune, ret *time.Time) (int, error) {
					return timePrecisionParser(precision, target, ret)
				},
				Format: func(t *time.Time) ([]rune, error) {
					return timePrecisionFormatter(precision, t)
				},
			}, 2, nil
		}
	case '4':
		if len(format) > 1 {
			switch format[1] {
			case 'S':
				return &CombinationFormatTimeInfo{
					AvailableTypes: []TimeFormatType{
						FormatTypeTime,
						FormatTypeDatetime,
						FormatTypeTimestamp,
					},
					Parse: func(target []rune, ret *time.Time) (int, error) {
						return timePrecisionParser(4, target, ret)
					},
					Format: func(t *time.Time) ([]rune, error) {
						return timePrecisionFormatter(4, t)
					},
				}, 2, nil
			case 'Y':
				return &CombinationFormatTimeInfo{
					AvailableTypes: []TimeFormatType{
						FormatTypeDate,
						FormatTypeDatetime,
						FormatTypeTimestamp,
					},
					Parse:  yearParser,
					Format: timeYear4Formatter,
				}, 2, nil
			}
		}
	case '*':
		if len(format) > 1 && format[1] == 'S' {
			return &CombinationFormatTimeInfo{
				AvailableTypes: []TimeFormatType{
					FormatTypeTime,
					FormatTypeDatetime,
					FormatTypeTimestamp,
				},
				Parse: func(target []rune, ret *time.Time) (int, error) {
					return timePrecisionParser(6, target, ret)
				},
				Format: func(t *time.Time) ([]rune, error) {
					return timePrecisionFormatter(6, t)
				},
			}, 2, nil
		}
	}
	return nil, 0, fmt.Errorf("unexpected format type %%%c", format[0])
}

func timeZoneRFC3339Parser(target []rune, t *time.Time) (int, error) {
	targetIdx := 0
	if target[targetIdx] == 'Z' {
		targetIdx++
		*t = time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			t.Second(),
			t.Nanosecond(),
			time.UTC,
		)
		return targetIdx, nil
	}
	if target[targetIdx] == '+' || target[targetIdx] == '-' {
		s := target[targetIdx]
		targetIdx++
		fmtLen := len("00:00")
		if len(target[targetIdx:]) != fmtLen {
			return 0, fmt.Errorf("unexpected offset format")
		}
		splitted := strings.Split(string(target[targetIdx:]), ":")
		if len(splitted) != 2 {
			return 0, fmt.Errorf("unexpected offset format")
		}
		hour := splitted[0]
		minute := splitted[1]
		if len(hour) != 2 || len(minute) != 2 {
			return 0, fmt.Errorf("unexpected hour:minute format")
		}
		h, err := strconv.ParseInt(hour, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("unexpected hour:minute format: %w", err)
		}
		m, err := strconv.ParseInt(minute, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("unexpected hour:minute format: %w", err)
		}
		hs := int(h*60*60 + m*60)
		if s == '-' {
			hs *= -1
		}
		*t = time.Date(
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			t.Second(),
			t.Nanosecond(),
			time.FixedZone("", hs),
		)
		targetIdx += fmtLen
		return targetIdx, nil
	}
	return 0, fmt.Errorf("unexpected offset format: %%%c", target[targetIdx])
}

func timeZoneRFC3339Formatter(t *time.Time) ([]rune, error) {
	return []rune(t.Format("-07:00")), nil
}

var timePrecisionMatcher = regexp.MustCompile(`[0-9]{2}\.?[0-9]*`)

func timePrecisionParser(precision int, text []rune, t *time.Time) (int, error) {
	const maxNanosecondsLength = 9
	extracted := timePrecisionMatcher.FindString(string(text))
	if len(extracted) == 0 {
		return 0, fmt.Errorf("failed to parse seconds.nanoseconds for %s", string(text))
	}
	fmtLen := len(extracted)
	splitted := strings.Split(extracted, ".")
	seconds := splitted[0]
	nanoseconds := strconv.Itoa(t.Nanosecond())
	if len(splitted) == 2 {
		nanoseconds = splitted[1]
		if len(nanoseconds) > precision {
			nanoseconds = nanoseconds[:precision]
		}
		nanoseconds = nanoseconds + strings.Repeat("0", maxNanosecondsLength-len(nanoseconds))
	}
	s, err := strconv.ParseInt(seconds, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse seconds parameter for %s: %w", string(text), err)
	}
	n, err := strconv.ParseInt(nanoseconds, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse nanoseconds parameter for %s: %w", string(text), err)
	}
	*t = time.Date(
		int(t.Year()),
		t.Month(),
		int(t.Day()),
		int(t.Hour()),
		int(t.Minute()),
		int(s),
		int(n),
		t.Location(),
	)
	return fmtLen, nil
}

func timePrecisionFormatter(precision int, t *time.Time) ([]rune, error) {
	return []rune(t.Format(fmt.Sprintf("05.%s", strings.Repeat("0", precision)))), nil
}

func timeYear4Formatter(t *time.Time) ([]rune, error) {
	return []rune(t.Format("2006")), nil
}
