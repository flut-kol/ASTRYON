package api

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/astryon/file-ingestion/internal/listing"
)

var jobStatuses = map[string]bool{"NEW": true, "QUEUED": true, "RUNNING": true, "RETRYING": true, "SUCCEEDED": true,
	"FAILED": true, "CANCELLED": true, "QUARANTINED": true}

var labelNames = map[string]classify.Label{string(classify.Geo): classify.Geo,
	string(classify.Credentials): classify.Credentials, string(classify.PII): classify.PII}

// parseListQuery validates the query string; any unknown parameter, repeated single-value parameter or bad value is rejected.
func parseListQuery(q url.Values) (listing.Filter, bool) {
	var f listing.Filter
	single := func(name string) (string, bool, bool) {
		v := q[name]
		if len(v) == 0 {
			return "", false, true
		}
		if len(v) > 1 {
			return "", false, false
		}
		return v[0], true, true
	}
	for k := range q {
		switch k {
		case "limit", "cursor", "source_type", "sensitivity", "status", "label", "tag", "created_from", "created_to":
		default:
			return f, false
		}
	}
	var ok, set bool
	var v string
	if v, set, ok = single("limit"); !ok {
		return f, false
	} else if set {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > listing.MaxLimit {
			return f, false
		}
		f.Limit = n
	}
	if v, set, ok = single("cursor"); !ok || (set && (v == "" || len(v) > 256)) {
		return f, false
	} else if set {
		f.Cursor = v
	}
	if v, set, ok = single("source_type"); !ok || (set && !typeRe.MatchString(v)) {
		return f, false
	} else if set {
		f.SourceType = v
	}
	if v, set, ok = single("sensitivity"); !ok || (set && v != "public" && v != "internal" && v != "restricted") {
		return f, false
	} else if set {
		f.Sensitivity = v
	}
	if v, set, ok = single("status"); !ok || (set && !jobStatuses[v]) {
		return f, false
	} else if set {
		f.Status = v
	}
	if v, set, ok = single("label"); !ok {
		return f, false
	} else if set {
		if f.Label, ok = labelNames[v]; !ok {
			return f, false
		}
	}
	for name, dst := range map[string]*time.Time{"created_from": &f.From, "created_to": &f.To} {
		if v, set, ok = single(name); !ok {
			return f, false
		} else if set {
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return f, false
			}
			*dst = t
		}
	}
	if !f.From.IsZero() && !f.To.IsZero() && !f.From.Before(f.To) {
		return f, false
	}
	if len(q["tag"]) > listing.MaxTagFilter {
		return f, false
	}
	for _, raw := range q["tag"] {
		k, val, has := strings.Cut(raw, ":") // value may itself contain ':'
		if !tagKeyRe.MatchString(k) || (has && !tagValRe.MatchString(val)) {
			return f, false
		}
		f.Tags = append(f.Tags, listing.Tag{Key: k, Value: val, HasValue: has})
	}
	return f, true
}

type listItem struct {
	FileObjectID string            `json:"file_object_id"`
	SourceID     string            `json:"source_id"`
	SourceType   string            `json:"source_type"`
	Committed    bool              `json:"committed"`
	SizeBytes    int64             `json:"size_bytes"`
	Sensitivity  string            `json:"sensitivity"`
	Tags         map[string]string `json:"tags"`
	CreatedAt    time.Time         `json:"created_at"`
	Job          *jobResp          `json:"job,omitempty"`
}

type listResp struct {
	Items      []listItem `json:"items"`
	NextCursor string     `json:"next_cursor,omitempty"`
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	p, ok := s.principal(w, r)
	if !ok {
		return
	}
	f, ok := parseListQuery(r.URL.Query())
	if !ok {
		writeErr(w, 400, "BODY_INVALID")
		return
	}
	if s.Lister == nil {
		writeErr(w, 503, "UNAVAILABLE")
		return
	}
	page, err := s.Lister.List(r.Context(), p.TenantID, f)
	if errors.Is(err, listing.ErrBadCursor) {
		writeErr(w, 400, "BODY_INVALID")
		return
	}
	if err != nil {
		writeErr(w, 503, "UNAVAILABLE")
		return
	}
	out := listResp{Items: make([]listItem, 0, len(page.Items)), NextCursor: page.NextCursor}
	for _, it := range page.Items {
		li := listItem{FileObjectID: it.FileObjectID, SourceID: it.SourceID, SourceType: it.SourceType, Committed: it.Committed,
			SizeBytes: it.SizeBytes, Sensitivity: it.Sensitivity, Tags: it.Tags, CreatedAt: it.CreatedAt}
		if li.Tags == nil {
			li.Tags = map[string]string{}
		}
		if it.JobID != "" {
			li.Job = &jobResp{ID: it.JobID, Status: it.Status, Error: it.Error, Labels: it.Labels}
		}
		out.Items = append(out.Items, li)
	}
	writeJSON(w, 200, out)
}
