package zetasqlite

import (
	"math"
	"strconv"
	"time"
)

// TimeFromTimestampValue zetasqlite returns string values ​​by default for timestamp values.
// This function is a helper function to convert that value to time.Time type.
func TimeFromTimestampValue(v string) (time.Time, error) {
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return time.Time{}, err
	}
	secs := math.Trunc(f)
	micros := math.Trunc((f-secs)*1e6 + 0.5)
	return time.Unix(int64(secs), int64(micros)*1000).UTC(), nil
}
