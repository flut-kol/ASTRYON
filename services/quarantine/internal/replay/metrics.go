package replay

import (
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
)

// Metrics are process-wide counters rendered as Prometheus text. Label sets are fixed; there are no
// tenant, source or job labels.
var Metrics metricsSet

var (
	browseOutcomes  = []string{"ok", "unauthorized", "invalid_request", "unavailable"}
	planResults     = []string{"executable", "over_limit", "unavailable"}
	publishOutcomes = []string{"published", "rejected", "plan_unavailable", "scope_too_large", "audit_unavailable", "publish_failed", "unavailable"}
	journalOutcomes = []string{"inserted", "dlq", "error"}
	candidateLE     = []int64{0, 1, 10, 100, 500, 1000, 2500, 5000, 10000}
)

type metricsSet struct {
	browse         [4]atomic.Int64
	plans          [3]atomic.Int64
	publish        [7]atomic.Int64
	candBkt        [10]atomic.Int64 // per-bucket, last is +Inf
	candCnt        atomic.Int64
	candSum        atomic.Int64
	mirrorOK       atomic.Int64
	mirrorErrors   atomic.Int64
	journal        [3]atomic.Int64
	journalCommits atomic.Int64
}

func (m *metricsSet) incMirror(ok bool) {
	if ok {
		m.mirrorOK.Add(1)
	} else {
		m.mirrorErrors.Add(1)
	}
}

func idx(set []string, v string) int {
	for i, x := range set {
		if x == v {
			return i
		}
	}
	return -1
}

func (m *metricsSet) incBrowse(o string) {
	if i := idx(browseOutcomes, o); i >= 0 {
		m.browse[i].Add(1)
	}
}

func (m *metricsSet) incPlan(r string) {
	if i := idx(planResults, r); i >= 0 {
		m.plans[i].Add(1)
	}
}

func (m *metricsSet) incPublish(o string) {
	if i := idx(publishOutcomes, o); i >= 0 {
		m.publish[i].Add(1)
	}
}

func (m *metricsSet) incJournal(o string) {
	if i := idx(journalOutcomes, o); i >= 0 {
		m.journal[i].Add(1)
	}
}

func (m *metricsSet) observeCandidates(n int64) {
	b := len(candidateLE)
	for i, le := range candidateLE {
		if n <= le {
			b = i
			break
		}
	}
	m.candBkt[b].Add(1)
	m.candCnt.Add(1)
	m.candSum.Add(n)
}

// statusOutcome maps a browse response status to a bounded outcome.
func statusOutcome(status int) string {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return "unauthorized"
	case status >= 500:
		return "unavailable"
	case status >= 400:
		return "invalid_request"
	}
	return "ok"
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(c int) { s.status = c; s.ResponseWriter.WriteHeader(c) }

// WritePrometheus renders all quarantine metrics.
func (m *metricsSet) WritePrometheus(w io.Writer) {
	fmt.Fprint(w, "# HELP astryon_quarantine_browse_requests_total Browse requests by outcome\n# TYPE astryon_quarantine_browse_requests_total counter\n")
	for i, o := range browseOutcomes {
		fmt.Fprintf(w, "astryon_quarantine_browse_requests_total{outcome=%q} %d\n", o, m.browse[i].Load())
	}
	fmt.Fprint(w, "# HELP astryon_quarantine_replay_dry_runs_total Replay dry-run plans by result\n# TYPE astryon_quarantine_replay_dry_runs_total counter\n")
	for i, o := range planResults {
		fmt.Fprintf(w, "astryon_quarantine_replay_dry_runs_total{result=%q} %d\n", o, m.plans[i].Load())
	}
	fmt.Fprint(w, "# HELP astryon_quarantine_replay_publishes_total Replay publish requests by outcome\n# TYPE astryon_quarantine_replay_publishes_total counter\n")
	for i, o := range publishOutcomes {
		fmt.Fprintf(w, "astryon_quarantine_replay_publishes_total{outcome=%q} %d\n", o, m.publish[i].Load())
	}
	fmt.Fprint(w, "# HELP astryon_quarantine_replay_candidates Candidate count per computed replay plan (capped by the count limit)\n# TYPE astryon_quarantine_replay_candidates histogram\n")
	var cum int64
	for i, le := range candidateLE {
		cum += m.candBkt[i].Load()
		fmt.Fprintf(w, "astryon_quarantine_replay_candidates_bucket{le=\"%d\"} %d\n", le, cum)
	}
	cum += m.candBkt[len(candidateLE)].Load()
	fmt.Fprintf(w, "astryon_quarantine_replay_candidates_bucket{le=\"+Inf\"} %d\nastryon_quarantine_replay_candidates_sum %d\nastryon_quarantine_replay_candidates_count %d\n",
		cum, m.candSum.Load(), m.candCnt.Load())
	fmt.Fprintf(w, "# HELP astryon_quarantine_audit_mirror_total Optional PG audit mirror writes\n# TYPE astryon_quarantine_audit_mirror_total counter\nastryon_quarantine_audit_mirror_total{outcome=\"written\"} %d\nastryon_quarantine_audit_mirror_total{outcome=\"error\"} %d\n", m.mirrorOK.Load(), m.mirrorErrors.Load())
	fmt.Fprint(w, "# HELP astryon_quarantine_journal_records_total Journal records by outcome\n# TYPE astryon_quarantine_journal_records_total counter\n")
	for i, o := range journalOutcomes {
		fmt.Fprintf(w, "astryon_quarantine_journal_records_total{outcome=%q} %d\n", o, m.journal[i].Load())
	}
	fmt.Fprintf(w, "# HELP astryon_quarantine_journal_commit_errors_total Journal Kafka offset commit failures\n# TYPE astryon_quarantine_journal_commit_errors_total counter\nastryon_quarantine_journal_commit_errors_total %d\n", m.journalCommits.Load())
}
