package auth

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

// fakeRedis answers AUTH/SET NX EX like Redis for the commands the store sends.
func fakeRedis(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	seen := map[string]bool{}
	var mu = make(chan struct{}, 1)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					n := 0
					for _, ch := range strings.TrimSpace(line)[1:] {
						n = n*10 + int(ch-'0')
					}
					args := make([]string, n)
					for i := range args {
						r.ReadString('\n')
						args[i], _ = r.ReadString('\n')
						args[i] = strings.TrimRight(args[i], "\r\n")
					}
					mu <- struct{}{}
					switch {
					case args[0] == "AUTH" && args[len(args)-1] == "pw":
						c.Write([]byte("+OK\r\n"))
					case args[0] == "AUTH":
						c.Write([]byte("-WRONGPASS\r\n"))
					case args[0] == "SET" && args[3] == "NX" && args[4] == "EX" && !seen[args[1]]:
						seen[args[1]] = true
						c.Write([]byte("+OK\r\n"))
					case args[0] == "SET":
						c.Write([]byte("$-1\r\n"))
					}
					<-mu
				}
			}()
		}
	}()
	return l.Addr().String()
}

func TestRedisReplayStore(t *testing.T) {
	addr := fakeRedis(t)
	s, err := NewRedisReplayStore("redis://u:pw@"+addr+"/0", "")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if ok, err := s.First(ctx, "k", time.Minute); err != nil || !ok {
		t.Fatalf("first: %v %v", ok, err)
	}
	if ok, err := s.First(ctx, "k", time.Minute); err != nil || ok {
		t.Fatalf("replay: %v %v", ok, err)
	}
	bad, _ := NewRedisReplayStore("redis://u:nope@"+addr, "")
	if _, err := bad.First(ctx, "z", time.Minute); err == nil {
		t.Fatal("wrong password must fail closed")
	}
	down, _ := NewRedisReplayStore("redis://127.0.0.1:1", "")
	if _, err := down.First(ctx, "z", time.Minute); err == nil {
		t.Fatal("outage must fail closed")
	}
	for _, u := range []string{"http://x", "redis://", "redis://h:1/abc"} {
		if _, err := NewRedisReplayStore(u, ""); err == nil {
			t.Errorf("%q accepted", u)
		}
	}
}
