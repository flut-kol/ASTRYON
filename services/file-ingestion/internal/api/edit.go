package api

import (
	"errors"
	"net/http"
	"regexp"
	"time"

	"github.com/astryon/file-ingestion/internal/editsave"
	"github.com/astryon/file-ingestion/internal/objects"
	"github.com/astryon/file-ingestion/internal/transform"
)

var editFilenameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$`)

type formatReq struct {
	InputHeaderRow  *bool  `json:"input_header_row,omitempty"`
	OutputHeaderRow *bool  `json:"output_header_row,omitempty"`
	OutputDelimiter string `json:"output_delimiter,omitempty"`
}

// resolve validates the request's format fields and applies the documented defaults (both header rows true,
// delimiter comma); ok is false on an invalid output_delimiter (anything but exactly one byte).
func (f formatReq) resolve() (editsave.FormatOptions, bool) {
	delim := byte(0)
	if f.OutputDelimiter != "" {
		if len(f.OutputDelimiter) != 1 {
			return editsave.FormatOptions{}, false
		}
		delim = f.OutputDelimiter[0]
	}
	boolOr := func(p *bool, def bool) bool {
		if p == nil {
			return def
		}
		return *p
	}
	return editsave.FormatOptions{
		InputHeaderRow: boolOr(f.InputHeaderRow, true), OutputHeaderRow: boolOr(f.OutputHeaderRow, true),
		OutputDelimiter: delim,
	}, true
}

type previewReq struct {
	formatReq
	Ops   []transform.OpSpec `json:"ops"`
	Limit int                `json:"limit,omitempty"`
}

type saveReq struct {
	formatReq
	Ops      []transform.OpSpec `json:"ops"`
	Filename string             `json:"filename,omitempty"`
}

type previewResp struct {
	Header    []string           `json:"header"`
	Rows      [][]string         `json:"rows"`
	Truncated bool               `json:"truncated"`
	Warnings  []editsave.Warning `json:"warnings"`
}

type versionResp struct {
	VersionID         string     `json:"version_id"`
	FileObjectID      string     `json:"file_object_id"`
	VersionNumber     int        `json:"version_number"`
	Header            []string   `json:"header"`
	OutputDelimiter   string     `json:"output_delimiter"`
	HeaderRow         bool       `json:"header_row"`
	SizeBytes         int64      `json:"size_bytes"`
	RowCount          int64      `json:"row_count"`
	DownloadURL       string     `json:"download_url,omitempty"`
	DownloadExpiresAt *time.Time `json:"download_expires_at,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
}

type versionListResp struct {
	Items []versionResp `json:"items"`
}

// writeEditErr maps an editsave/transform error to the public catalog; it returns true iff err is nil.
func writeEditErr(w http.ResponseWriter, err error) bool {
	switch {
	case err == nil:
		return true
	case errors.Is(err, objects.ErrNotFound):
		writeErr(w, 404, "NOT_FOUND")
	case errors.Is(err, editsave.ErrNotCommitted):
		writeErr(w, 409, "FILE_NOT_COMMITTED")
	case errors.Is(err, editsave.ErrUnsupportedContentType):
		writeErr(w, 422, "UNSUPPORTED_CONTENT_TYPE")
	case errors.Is(err, editsave.ErrEditTooLarge):
		writeErr(w, 413, "EDIT_TOO_LARGE")
	case errors.Is(err, editsave.ErrVersionConflict):
		writeErr(w, 409, "VERSION_CONFLICT")
	case errors.Is(err, transform.ErrOpsInvalid):
		writeErr(w, 422, "OPS_INVALID")
	default:
		writeErr(w, 503, "UNAVAILABLE")
	}
	return false
}

func (s *Server) editsAvailable(w http.ResponseWriter) bool {
	if s.Edits.Versions == nil {
		writeErr(w, 503, "UNAVAILABLE")
		return false
	}
	return true
}

// versionResp builds the public summary; withDownload presigns a fresh, short-lived download link.
func (s *Server) versionResp(v editsave.Version, withDownload bool) (versionResp, bool) {
	out := versionResp{VersionID: v.ID, FileObjectID: v.FileObjectID, VersionNumber: v.VersionNumber, Header: v.Header,
		OutputDelimiter: v.OutputDelimiter, HeaderRow: v.OutputHeaderRow, SizeBytes: v.SizeBytes, RowCount: v.RowCount,
		CreatedAt: v.CreatedAt}
	if withDownload {
		url, exp, err := s.Edits.PresignDownload(v.ObjectKey)
		if err != nil {
			return versionResp{}, false
		}
		out.DownloadURL, out.DownloadExpiresAt = url, &exp
	}
	return out, true
}

func (s *Server) preview(w http.ResponseWriter, r *http.Request) {
	p, ok := s.principal(w, r)
	if !ok {
		return
	}
	if !s.editsAvailable(w) {
		return
	}
	id := r.PathValue("id")
	if !uuidRe.MatchString(id) {
		writeErr(w, 404, "NOT_FOUND")
		return
	}
	var in previewReq
	if !decode(w, r, &in) {
		return
	}
	fmtOpts, ok := in.resolve()
	if !ok {
		writeErr(w, 400, "BODY_INVALID")
		return
	}
	res, err := s.Edits.Preview(r.Context(), p.TenantID, id, in.Ops, fmtOpts, in.Limit)
	if !writeEditErr(w, err) {
		return
	}
	writeJSON(w, 200, previewResp{Header: res.Header, Rows: res.Rows, Truncated: res.Truncated, Warnings: res.Warnings})
}

func (s *Server) createVersion(w http.ResponseWriter, r *http.Request) {
	p, ok := s.principal(w, r)
	if !ok {
		return
	}
	if !s.editsAvailable(w) {
		return
	}
	id := r.PathValue("id")
	if !uuidRe.MatchString(id) {
		writeErr(w, 404, "NOT_FOUND")
		return
	}
	var in saveReq
	if !decode(w, r, &in) {
		return
	}
	if in.Filename != "" && !editFilenameRe.MatchString(in.Filename) {
		writeErr(w, 400, "BODY_INVALID")
		return
	}
	fmtOpts, ok := in.resolve()
	if !ok {
		writeErr(w, 400, "BODY_INVALID")
		return
	}
	v, err := s.Edits.Save(r.Context(), p.TenantID, id, in.Ops, fmtOpts, in.Filename, "")
	if !writeEditErr(w, err) {
		return
	}
	out, ok := s.versionResp(v, true)
	if !ok {
		writeErr(w, 503, "UNAVAILABLE")
		return
	}
	writeJSON(w, 201, out)
}

func (s *Server) listVersions(w http.ResponseWriter, r *http.Request) {
	p, ok := s.principal(w, r)
	if !ok {
		return
	}
	if !s.editsAvailable(w) {
		return
	}
	id := r.PathValue("id")
	if !uuidRe.MatchString(id) {
		writeErr(w, 404, "NOT_FOUND")
		return
	}
	vs, err := s.Edits.Versions.List(r.Context(), p.TenantID, id)
	if err != nil {
		writeErr(w, 503, "UNAVAILABLE")
		return
	}
	items := make([]versionResp, len(vs))
	for i, v := range vs {
		items[i], _ = s.versionResp(v, false)
	}
	writeJSON(w, 200, versionListResp{Items: items})
}

func (s *Server) getVersion(w http.ResponseWriter, r *http.Request) {
	p, ok := s.principal(w, r)
	if !ok {
		return
	}
	if !s.editsAvailable(w) {
		return
	}
	id, versionID := r.PathValue("id"), r.PathValue("versionId")
	if !uuidRe.MatchString(id) || !uuidRe.MatchString(versionID) {
		writeErr(w, 404, "NOT_FOUND")
		return
	}
	v, err := s.Edits.Versions.Get(r.Context(), p.TenantID, id, versionID)
	if errors.Is(err, editsave.ErrNotFound) {
		writeErr(w, 404, "NOT_FOUND")
		return
	}
	if err != nil {
		writeErr(w, 503, "UNAVAILABLE")
		return
	}
	out, ok := s.versionResp(v, true)
	if !ok {
		writeErr(w, 503, "UNAVAILABLE")
		return
	}
	writeJSON(w, 200, out)
}
