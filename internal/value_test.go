package internal

import "testing"

func TestTimestampValue(t *testing.T) {
	if !timestampRe.MatchString("2022-01-01 00:00:00+00") {
		t.Fatalf("mismatch timestamp value")
	}
	if !timestampRe.MatchString("2022-01-01T00:00:00+00") {
		t.Fatalf("mismatch timestamp value")
	}
	if !datetimeRe.MatchString("2022-01-01 00:00:00") {
		t.Fatalf("mismatch timestamp value")
	}
	formatted, err := formatTimestamp("2022-01-01 00:00:00")
	if err != nil {
		t.Fatal(err)
	}
	if formatted != "2022-01-01 00:00:00+00" {
		t.Fatalf("failed to format timestamp")
	}
}
