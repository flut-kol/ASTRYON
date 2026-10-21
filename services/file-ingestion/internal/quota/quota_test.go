package quota

import (
	"context"
	"errors"
	"testing"
)

type fake struct {
	ov  map[string]int64
	u   Usage
	err error
}

func (f fake) Overrides(context.Context, string) (map[string]int64, error) { return f.ov, f.err }
func (f fake) Usage(context.Context, string) (Usage, error)                { return f.u, f.err }

func TestCheck(t *testing.T) {
	d := Limits{MaxFileBytes: 100, MaxOpen: 2, BytesPerDay: 150}
	cases := []struct {
		name string
		f    fake
		size int64
		want error
	}{
		{"ok", fake{}, 50, nil},
		{"too large", fake{}, 101, ErrFileTooLarge},
		{"open full", fake{u: Usage{Open: 2}}, 10, ErrTooManyOpen},
		{"open below", fake{u: Usage{Open: 1}}, 10, nil},
		{"day exceeded", fake{u: Usage{Bytes: 100}}, 51, ErrDailyBytesFull},
		{"day exact", fake{u: Usage{Bytes: 100}}, 50, nil},
		{"override raises file max", fake{ov: map[string]int64{ResFileMax: 500, ResBytesDay: 0}}, 400, nil},
		{"override lowers open", fake{ov: map[string]int64{ResOpenMax: 1}, u: Usage{Open: 1}}, 1, ErrTooManyOpen},
		{"override zero = unlimited bytes", fake{ov: map[string]int64{ResBytesDay: 0}, u: Usage{Bytes: 1 << 40}}, 10, nil},
		{"store error fails closed", fake{err: errors.New("db")}, 1, errors.New("db")},
	}
	for _, c := range cases {
		err := Checker{Store: c.f, Defaults: d}.Check(context.Background(), "t", c.size)
		if (err == nil) != (c.want == nil) || (err != nil && c.want != nil && err.Error() != c.want.Error()) {
			t.Errorf("%s: got %v want %v", c.name, err, c.want)
		}
	}
}

func TestUnlimitedSkipsUsage(t *testing.T) {
	if err := (Checker{Store: fake{err: errors.New("must not be read")}}).Check(context.Background(), "t", 1); err == nil {
		// Overrides is still read (fails closed); with a healthy store and zero limits nothing else is consulted.
		t.Fatal("expected overrides error to fail closed")
	}
	if err := (Checker{Store: fake{u: Usage{Open: 99, Bytes: 1 << 50}}}).Check(context.Background(), "t", 1<<40); err != nil {
		t.Fatal(err)
	}
}
