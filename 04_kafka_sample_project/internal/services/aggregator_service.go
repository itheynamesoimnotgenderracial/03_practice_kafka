package services

import "time"

func HourWindow(t time.Time) time.Time {
	return t.UTC().Truncate(time.Hour)
}

func DayWindow(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
