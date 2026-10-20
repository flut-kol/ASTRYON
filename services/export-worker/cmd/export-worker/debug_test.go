package main

import "testing"

func TestLoopbackAddr(t *testing.T) {
	for a, ok := range map[string]bool{"127.0.0.1:6060": true, "[::1]:6060": true, "localhost:6060": true,
		":6060": false, "0.0.0.0:6060": false, "10.1.2.3:6060": false, "example.com:6060": false, "6060": false} {
		if err := loopbackAddr(a); (err == nil) != ok {
			t.Errorf("%s: err=%v want ok=%v", a, err, ok)
		}
	}
}
