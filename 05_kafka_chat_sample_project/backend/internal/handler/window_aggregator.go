package handler

import "time"

func ComputeHourlyWindow(timestamp int64) (start, end time.Time) {
	windowSize := time.Hour
	t := time.Unix(timestamp, 0).UTC()
	start = t.Truncate(windowSize)
	end = start.Add(windowSize)

	return
}

func ComputeDailylyWindow(timestamp int64) (start, end time.Time) {
	windowSize := time.Now().Day()
	t := time.Unix(timestamp, 0).UTC()
	start = t.Truncate(time.Duration(windowSize))
	end = start.Add(time.Duration(windowSize))
	return
}
