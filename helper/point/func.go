package point

import (
	"fmt"
	"math"
)

// CleanUp removes points with empty metric
// for run after Deduplicate, Merge, etc for result cleanup
func CleanUp(points []Point) []Point {
	l := len(points)
	squashed := 0

	for i := 0; i < l; i++ {
		if points[i].MetricID == 0 || math.IsNaN(points[i].Value) {
			squashed++
			continue
		}
		if squashed > 0 {
			points[i-squashed] = points[i]
		}
	}

	return points[:l-squashed]
}

// Uniq removes points with equal metric and time
func Uniq(points []Point) []Point {
	l := len(points)
	var i, n int
	// i - current position of iterator
	// n - position on first record with current key (metric + time)

	for i = 1; i < l; i++ {
		if points[i].MetricID != points[n].MetricID ||
			points[i].Time != points[n].Time {
			n = i
			continue
		}

		if points[i].Timestamp > points[n].Timestamp {
			points[n] = points[i]
		}

		points[i].MetricID = 0 // mark for remove
	}

	return CleanUp(points)
}

// FillNulls accepts an ordered []Point for one metric and returns a generator that will return all points for specific
// interval. Generator returns EmptyPoint when it's finished
func FillNulls(points []Point, from, until, step uint32, approximateAggregate bool) (start, stop, count uint32, getter GetValueOrNaN) {
	start = from - (from % step)
	if !approximateAggregate {
		if start < from {
			start += step
		}
		stop = until - (until % step) + step
	} else {
		stop = until - (until % step)
	}
	count = (stop - start) / step
	last := start - step
	currentPoint := 0
	fmt.Printf("Start %d, Stop %d, Until %d, From %d, Count %d, Last %d\n", start, stop, until, from, count, last)
	var metricID uint32
	if len(points) > 0 {
		metricID = points[0].MetricID
	}
	getter = func() (float64, error) {
		if stop <= last {
			return 0, ErrTimeGreaterStop
		}
		for i := currentPoint; i < len(points); i++ {
			point := points[i]
			//fmt.Printf("Point Time %d, last %d, start %d, stop %d\n", point.Time, last, start, stop)
			if metricID != point.MetricID {
				return 0, fmt.Errorf("the point MetricID %d differs from other %d: %w", point.MetricID, metricID, ErrWrongMetricID)
			}
			if point.Time < start {
				//fmt.Println("Skipped Time < start")
				// Points begin before request's start
				currentPoint++
				continue
			}
			if point.Time <= last {
				// This is definitely an error. Possible reason is unsorted points
				return 0, fmt.Errorf("the time is less or equal to previous %d < %d: %w", point.Time, last, ErrPointsUnsorted)
			}
			if stop <= point.Time {
				//fmt.Println("stop <= Point.Time")
				break
			}
			if last+step < point.Time {
				//fmt.Println("Last+step < pTime")
				// There are nulls in slice
				last += step
				return math.NaN(), nil
			}
			//fmt.Println("Last but not least")
			last = point.Time
			currentPoint = i + 1
			return point.Value, nil
		}
		if last+step < stop {
			//fmt.Println("Still Not done")
			last += step
			return math.NaN(), nil
		}
		return 0, ErrTimeGreaterStop
	}
	return
}
