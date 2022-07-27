package tsdb

import (
	"testing"
	"time"
)

func TestDateRange_OverlapsClosedInterval(t *testing.T) {
	for i := 0; i < 60; i++ {
		format := "2006-01-02 15:04:05"
		now, _ := time.ParseInLocation(format, "2021-05-28 15:04:05", time.Local)
		r := DeleteIgnore{
			start: NewDateExpr("*-05-30"),
			end:   NewDateExpr("*-06-19"),
		}

		start := now.Add(time.Duration(i*24) * time.Hour)
		over := r.OverlapsClosedInterval(start.Unix()*1000, start.Unix()*1000+time.Hour.Milliseconds())

		t.Log("time:", start, "result:", over)
	}
}
