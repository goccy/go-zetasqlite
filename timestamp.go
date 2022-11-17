package zetasqlite

import (
	"strconv"
	"time"

	"github.com/goccy/go-zetasqlite/internal"
)

// TimeFromTimestampValue zetasqlite returns string values ​​by default for timestamp values.
// This function is a helper function to convert that value to time.Time type.
func TimeFromTimestampValue(v string) (time.Time, error) {
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return time.Time{}, err
	}
	return internal.TimestampFromFloatValue(f)
}
