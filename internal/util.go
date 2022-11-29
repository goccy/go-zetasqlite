package internal

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

var (
	timeZoneOffsetPartialPattern = regexp.MustCompile(`([-+][0-9]{2})`)
	timeZoneOffsetPattern        = regexp.MustCompile(`([-+][0-9]{2}):([0-9]{2})`)
)

func toLocation(timeZone string) (*time.Location, error) {
	if matched := timeZoneOffsetPattern.FindAllStringSubmatch(timeZone, -1); len(matched) != 0 && len(matched[0]) == 3 {
		offsetHour := matched[0][1]
		offsetMin := matched[0][2]
		hour, err := strconv.ParseInt(offsetHour, 10, 64)
		if err != nil {
			return nil, err
		}
		min, err := strconv.ParseInt(offsetMin, 10, 64)
		if err != nil {
			return nil, err
		}
		return time.FixedZone(
			fmt.Sprintf("UTC%s", timeZone),
			int(hour)*60*60+int(min)*60,
		), nil
	}
	if matched := timeZoneOffsetPartialPattern.FindAllStringSubmatch(timeZone, -1); len(matched) != 0 && len(matched[0]) == 2 {
		offset := matched[0][1]
		hour, err := strconv.ParseInt(offset, 10, 64)
		if err != nil {
			return nil, err
		}
		return time.FixedZone(
			fmt.Sprintf("UTC%s", timeZone),
			int(hour)*60*60,
		), nil
	}

	loc, err := time.LoadLocation(timeZone)
	if err != nil {
		return nil, fmt.Errorf("failed to load location from %s: %w", timeZone, err)
	}
	return loc, nil
}

func modifyTimeZone(t time.Time, loc *time.Location) (time.Time, error) {
	// remove timezone parameter from time
	format := t.Format("2006-01-02T15:04:05.999999999")
	return parseTimestamp(format, loc)
}
