package adminapi

import (
	"net/http"
	"strconv"

	goauth "github.com/astryon/go-common/auth"
)

func (s *Server) listSources(w http.ResponseWriter, r *http.Request, p goauth.Principal) {
	if s.Sources == nil {
		writeErr(w, r, http.StatusServiceUnavailable, "unavailable", "sources store not configured")
		return
	}
	q := r.URL.Query()
	for k := range q {
		if k != "limit" && k != "cursor" {
			writeErr(w, r, http.StatusBadRequest, "invalid_request", "unknown query parameter "+k)
			return
		}
	}
	limit := defaultLimit
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxLimit {
			writeErr(w, r, http.StatusBadRequest, "invalid_request", "limit must be 1.."+strconv.Itoa(maxLimit))
			return
		}
		limit = n
	}
	after := q.Get("cursor")
	if after != "" && !uuidRE.MatchString(after) {
		writeErr(w, r, http.StatusBadRequest, "invalid_request", "invalid cursor")
		return
	}
	items, err := s.Sources.ListSources(r.Context(), p.TenantID, after, limit+1)
	if err != nil {
		storeErr(w, r, err)
		return
	}
	next := ""
	if len(items) > limit {
		items = items[:limit]
		next = items[limit-1].ID
	}
	if items == nil {
		items = []Source{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

func (s *Server) getSource(w http.ResponseWriter, r *http.Request, p goauth.Principal) {
	if s.Sources == nil {
		writeErr(w, r, http.StatusServiceUnavailable, "unavailable", "sources store not configured")
		return
	}
	id := r.PathValue("id")
	if !uuidRE.MatchString(id) {
		writeErr(w, r, http.StatusBadRequest, "invalid_request", "invalid source id")
		return
	}
	src, err := s.Sources.GetSource(r.Context(), p.TenantID, id)
	if err != nil {
		storeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, src)
}
