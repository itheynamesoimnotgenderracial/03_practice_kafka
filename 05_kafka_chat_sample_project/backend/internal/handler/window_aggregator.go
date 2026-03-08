package handler

import "time"

func ComputeHourlyWindow(timestamp int64) (start, end time.Time) {
	t := toTime(timestamp)
	start = t.Truncate(time.Hour)
	end = start.Add(time.Hour)
	return
}

func ComputeDailyWindow(timestamp int64) (start, end time.Time) {
	t := toTime(timestamp)
	year, month, day := t.Date()
	start = time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	end = start.Add(24 * time.Hour)
	return
}

func toTime(timestamp int64) time.Time {
	if timestamp > 4102444800 {
		return time.Unix(timestamp/1000, (timestamp%1000)*int64(time.Millisecond)).UTC()
	}
	return time.Unix(timestamp, 0).UTC()
}
