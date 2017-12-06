package utils

import (
	"errors"
	"syscall"
)

/**
* Truncates the time to seconds.
 */
func MilliToSeconds(t int64) (int64, error) {

	msTime := t

	i := 0
	for {
		msTime = msTime / 10
		if msTime == 0 {
			break
		}
		i++
	}

	if i > 13 {
		return t, errors.New("the maximum resolution supported for timestamp is milliseconds")
	}

	if i > 10 {
		return (t / 1000) * 1000, nil
	} else if i < 10 {
		return t * 1000, nil
	}

	return t, nil
}

/**
* Returns the time truncated to seconds.
 */
func GetTimeNoMillis() int64 {

	var tv syscall.Timeval

	syscall.Gettimeofday(&tv)

	return int64(tv.Sec) * 1e3
}
