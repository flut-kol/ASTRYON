package main

import (
	"fmt"
	"sort"
	"time"
)

// Profile is a named, representative workload shape (DPP-056..058). It only
// sets generation options and batch/size defaults; rate, duration and
// concurrency stay under the scenario's control.
type Profile struct {
	Desc      string
	EventSize int
	Batch     int
	Opt       func(o *Options)
}

// Profiles are the workload profiles referenced by docs/benchmarks/load-plan.md.
var Profiles = map[string]Profile{
	"small-json": {Desc: "small flat JSON event, ~512 B (the 120k ev/s reference profile)", EventSize: 512, Batch: 100,
		Opt: func(o *Options) { o.Shape = "small" }},
	"wide-json": {Desc: "wide JSON, ~4 KiB, 120 fields with a nested object", EventSize: 4096, Batch: 50,
		Opt: func(o *Options) { o.Shape = "wide"; o.WideFields = 120 }},
	"skewed-tenants": {Desc: "Zipf(1.2) tenant skew plus 20% of events on one hot entity_key; needs several tenants", EventSize: 512, Batch: 100,
		Opt: func(o *Options) { o.TenantSkew = 1.2; o.HotKeyRatio = 0.2 }},
	"duplicates": {Desc: "10% re-sent events (same event_id and bytes); expects exactly-once effect downstream", EventSize: 512, Batch: 100,
		Opt: func(o *Options) { o.DupRatio = 0.10 }},
	"late-events": {Desc: "20% of events with event_time up to 7 days in the past", EventSize: 512, Batch: 100,
		Opt: func(o *Options) { o.LateRatio = 0.20; o.LateMax = 7 * 24 * time.Hour }},
	"invalid-schemas": {Desc: "5% schema-invalid events (schema_id empty / schema_version <= 0); expect quarantine, no 5xx", EventSize: 512, Batch: 100,
		Opt: func(o *Options) { o.InvalidRatio = 0.05; o.SchemaInvalid = true }},
	"mixed": {Desc: "blend: 60/40 small/wide is approximated by 1 KiB events with 5% dups, 5% late, 1% invalid", EventSize: 1024, Batch: 50,
		Opt: func(o *Options) {
			o.DupRatio = 0.05
			o.LateRatio = 0.05
			o.LateMax = 24 * time.Hour
			o.InvalidRatio = 0.01
		}},
}

// ProfileNames returns the sorted profile names.
func ProfileNames() []string {
	n := make([]string, 0, len(Profiles))
	for k := range Profiles {
		n = append(n, k)
	}
	sort.Strings(n)
	return n
}

// ApplyProfile sets cfg's Opt/BatchSize/EventSize from the named profile.
// Only values still at the flag default (setBatch/setSize false) are replaced,
// so explicit flags win over the profile.
func ApplyProfile(cfg *RunConfig, name string, setBatch, setSize bool) error {
	p, ok := Profiles[name]
	if !ok {
		return fmt.Errorf("unknown profile %q (have %v)", name, ProfileNames())
	}
	if !setSize {
		cfg.Opt.EventSize = p.EventSize
	}
	if !setBatch {
		cfg.BatchSize = p.Batch
	}
	size := cfg.Opt.EventSize
	p.Opt(&cfg.Opt)
	cfg.Opt.EventSize = size
	return nil
}
