package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// RunConfig is the load profile.
type RunConfig struct {
	URL         string
	Rate        float64 // events/s, 0 = unlimited
	Duration    time.Duration
	BatchSize   int
	Concurrency int
	Timeout     time.Duration
	Seed        int64
	Opt         Options
	Tenants     []Tenant
	TrackIDs    bool
}

type batchResp struct {
	Accepted    int `json:"accepted"`
	Quarantined int `json:"quarantined"`
	Failed      int `json:"failed"`
	Results     []struct {
		EventID string `json:"event_id"`
		Status  string `json:"status"`
	} `json:"results"`
	Code string `json:"code"`
}

// Run executes the load and returns stats plus wall time.
func Run(ctx context.Context, cfg RunConfig, client *http.Client) (*Stats, time.Duration) {
	st := NewStats(cfg.TrackIDs)
	lim := NewLimiter(cfg.Rate)
	ctx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			b := NewBuilder(cfg.Opt, cfg.Tenants, cfg.Seed+int64(w)*7919)
			for {
				if err := lim.WaitN(ctx, cfg.BatchSize); err != nil {
					return
				}
				batch := b.BuildBatch(cfg.BatchSize)
				// The request itself is not bound to the run deadline so in-flight batches finish.
				st.Record(post(client, cfg.URL, cfg.Timeout, batch))
			}
		}(w)
	}
	wg.Wait()
	return st, time.Since(start)
}

func post(client *http.Client, url string, timeout time.Duration, batch Batch) Outcome {
	o := Outcome{Events: batch.Events, Bytes: len(batch.Body)}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(batch.Body))
	if err != nil {
		o.Failed, o.Err = batch.Events, err.Error()
		return o
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+batch.Tenant.Token)
	t0 := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		o.Latency = time.Since(t0)
		o.Failed, o.Err = batch.Events, "transport: "+shortErr(err)
		return o
	}
	defer resp.Body.Close()
	body, rerr := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	o.Latency = time.Since(t0)
	o.HTTPStatus = resp.StatusCode
	var br batchResp
	if rerr != nil || json.Unmarshal(body, &br) != nil || (resp.StatusCode != 200 && resp.StatusCode != 207 && resp.StatusCode != 503) {
		o.Failed = batch.Events
		o.Err = fmt.Sprintf("http %d %s", resp.StatusCode, br.Code)
		return o
	}
	o.Accepted, o.Quarantined, o.Failed = br.Accepted, br.Quarantined, br.Failed
	if o.Failed > 0 {
		o.Err = fmt.Sprintf("http %d: %d events failed to publish", resp.StatusCode, o.Failed)
	}
	for _, r := range br.Results {
		if r.Status == "accepted" && r.EventID != "" {
			o.AcceptedIDs = append(o.AcceptedIDs, r.EventID)
		}
	}
	return o
}

func shortErr(err error) string {
	s := err.Error()
	if len(s) > 120 {
		s = s[:120]
	}
	return s
}
