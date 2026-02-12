package services

import "time"

func HourWindow(t time.Time) time.Time {
	return t.UTC().Truncate(time.Hour)
}

func DayWindow(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func HourlyWindow(t time.Time) (time.Time, time.Time) {
	start := t.Truncate(time.Hour)
	return start, start.Add(time.Hour)
}

func DailyWindow(t time.Time) (time.Time, time.Time) {
	start := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	return start, start.Add(24 * time.Hour)
}
