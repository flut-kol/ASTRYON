package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/astryon/connector-manager/internal/retrybridge"
)

// retryBackoff returns nil for CONNECTOR_RETRY_MODE=local (default): the runner keeps its built-in schedule.
func retryBackoff(getenv func(string) string) (func(string, int, error, time.Duration) (time.Duration, error), error) {
	switch m := getenv("CONNECTOR_RETRY_MODE"); m {
	case "", "local":
		return nil, nil
	case "controller":
		num := func(k string, d int) (int, error) {
			v := getenv(k)
			if v == "" {
				return d, nil
			}
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				return 0, fmt.Errorf("%s must be a positive integer", k)
			}
			return n, nil
		}
		base, err := num("CONNECTOR_RETRY_BASE_SECONDS", 30)
		if err != nil {
			return nil, err
		}
		max, err := num("CONNECTOR_RETRY_MAX_SECONDS", 3600)
		if err != nil {
			return nil, err
		}
		att, err := num("CONNECTOR_RETRY_MAX_ATTEMPTS", 10)
		if err != nil {
			return nil, err
		}
		b, err := retrybridge.New(time.Duration(base)*time.Second, time.Duration(max)*time.Second, att)
		if err != nil {
			return nil, err
		}
		return b.Delay, nil
	default:
		return nil, fmt.Errorf("CONNECTOR_RETRY_MODE must be local or controller, got %q", m)
	}
}
