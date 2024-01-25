package zetasqlite

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// TimeFromTimestampValue zetasqlite returns string values ​​by default for timestamp values.
// This function is a helper function to convert that value to time.Time type.
func TimeFromTimestampValue(v string) (time.Time, error) {
	// ParseFloat is too imprecise to use, instead split into seconds / microseconds
	parts := strings.Split(v, ".")
	if len(parts) > 2 {
		return time.Time{}, fmt.Errorf("cannot parse string with multiple delimiters")
	}
	seconds, err := strconv.ParseInt(parts[0], 10, 64)
	micros := int64(0)
	if len(parts) == 2 {
		micros, err = strconv.ParseInt(parts[1], 10, 64)
	}
	if err != nil {
		return time.Time{}, err
	}
	nanos := micros * int64(time.Microsecond)
	return time.Unix(seconds, nanos), err
}
