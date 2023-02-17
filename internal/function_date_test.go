package internal

import (
	"testing"
	"time"
)

func TestDateTrunc(t *testing.T) {
	t.Run("DAY", func(t *testing.T) {
		actualTime, _ := time.Parse(time.RFC3339, "2023-02-16T16:56:05Z")
		actualDateValue, _ := DATE_TRUNC(actualTime, "DAY")
		expectedTime, _ := time.Parse(time.RFC3339, "2023-02-16T00:00:00Z")
		expectedDateValue := DateValue(expectedTime)
		if expectedDateValue != actualDateValue {
			t.Fatalf("failed to date_trunc day")
		}
	})
}
