// Package classify labels parsed records as containing geo data, credentials or lightweight PII.
//
// It is built for the bounded parsers: a Scanner sees one record at a time (ScanJSON for an NDJSON line, ScanCSV for a
// header plus one CSV row) and keeps only per-label counters and a small capped set of field NAMES. Matched values are
// never stored, returned, logged or put into metrics; a field name that itself looks like a secret is replaced by a
// placeholder. Nothing buffers the file.
package classify

import (
	"encoding/json"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Label is a content classification.
type Label string

const (
	Geo         Label = "contains_geo"
	Credentials Label = "contains_credentials"
	PII         Label = "contains_pii"
)

// AllLabels is the closed label set (use it for metric label values).
var AllLabels = []Label{Geo, Credentials, PII}

const (
	maxFieldsPerLabel = 20
	maxFieldName      = 64
	maxValueScan      = 4096
	maxDepth          = 16
	maxNodes          = 20000
	Redacted          = "(redacted)"
)

// Finding is the evidence for one label: how many leaf values matched and which fields they were in.
type Finding struct {
	Count  int64    `json:"count"`
	Fields []string `json:"fields"`
}

// Labels maps a label to its finding. It is the exact JSON stored in jobs.content_labels.
type Labels map[Label]Finding

// Merge adds o into l (counts summed, field names unioned and capped).
func (l Labels) Merge(o Labels) {
	for k, f := range o {
		cur := l[k]
		cur.Count += f.Count
		cur.Fields = unionFields(cur.Fields, f.Fields)
		l[k] = cur
	}
}

// Has reports whether the label was found.
func (l Labels) Has(x Label) bool { return l[x].Count > 0 }

// Names returns the detected labels sorted.
func (l Labels) Names() []string {
	var out []string
	for k, f := range l {
		if f.Count > 0 {
			out = append(out, string(k))
		}
	}
	sort.Strings(out)
	return out
}

func unionFields(a, b []string) []string {
	seen := map[string]bool{}
	for _, x := range a {
		seen[x] = true
	}
	out := append([]string(nil), a...)
	for _, x := range b {
		if !seen[x] && len(out) < maxFieldsPerLabel {
			seen[x] = true
			out = append(out, x)
		}
	}
	sort.Strings(out)
	return out
}

// Scanner accumulates findings across records. Not safe for concurrent use.
type Scanner struct {
	found Labels
	only  map[Label]bool // nil = all detectors run
}

// NewScanner returns an empty scanner.
func NewScanner() *Scanner { return &Scanner{found: Labels{}} }

// NewScannerFor runs only the listed detectors (a source profile's selection); no labels = all.
func NewScannerFor(labels ...Label) *Scanner {
	s := NewScanner()
	if len(labels) > 0 {
		s.only = map[Label]bool{}
		for _, l := range labels {
			s.only[l] = true
		}
	}
	return s
}

// Take returns what was found since the last Take and resets the scanner.
func (s *Scanner) Take() Labels {
	out := s.found
	s.found = Labels{}
	return out
}

// Peek returns a copy of the findings so far.
func (s *Scanner) Peek() Labels {
	out := Labels{}
	out.Merge(s.found)
	return out
}

func (s *Scanner) hit(l Label, field string) {
	if s.only != nil && !s.only[l] {
		return
	}
	f := s.found[l]
	f.Count++
	field = safeName(field)
	f.Fields = unionFields(f.Fields, []string{field})
	s.found[l] = f
}

// ScanJSON classifies one JSON document (an NDJSON record). Invalid JSON is ignored (the parser quarantines it).
func (s *Scanner) ScanJSON(raw []byte) {
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return
	}
	nodes := 0
	s.walk("", v, 0, &nodes)
}

// ScanCSV classifies one CSV row; header[i] names fields[i]. Columns without a header name are called "col<N>".
func (s *Scanner) ScanCSV(header, fields []string) {
	for i, v := range fields {
		name := "col" + strconv.Itoa(i+1)
		if i < len(header) && header[i] != "" {
			name = header[i]
		}
		s.leaf(name, v)
	}
}

func (s *Scanner) walk(path string, v any, depth int, nodes *int) {
	*nodes++
	if depth > maxDepth || *nodes > maxNodes {
		return
	}
	switch x := v.(type) {
	case map[string]any:
		if isGeoJSON(x) {
			s.hit(Geo, orRoot(path))
			return
		}
		for k, c := range x {
			p := k
			if path != "" {
				p = path + "." + k
			}
			s.walk(p, c, depth+1, nodes)
		}
	case []any:
		for _, c := range x {
			s.walk(path+"[]", c, depth+1, nodes)
		}
	case string, float64:
		s.leaf(path, x)
	}
}

func orRoot(p string) string {
	if p == "" {
		return "$"
	}
	return p
}

var geoTypes = map[string]bool{"Point": true, "MultiPoint": true, "LineString": true, "MultiLineString": true, "Polygon": true,
	"MultiPolygon": true, "GeometryCollection": true, "Feature": true, "FeatureCollection": true}

func isGeoJSON(m map[string]any) bool {
	t, _ := m["type"].(string)
	if !geoTypes[t] {
		return false
	}
	switch t {
	case "Feature":
		_, ok := m["geometry"]
		return ok
	case "FeatureCollection":
		_, ok := m["features"].([]any)
		return ok
	case "GeometryCollection":
		_, ok := m["geometries"].([]any)
		return ok
	}
	_, ok := m["coordinates"].([]any)
	return ok
}

var (
	latName  = regexp.MustCompile(`(?i)(^|[^a-z])(lat|latitude)([^a-z]|$)`)
	lonName  = regexp.MustCompile(`(?i)(^|[^a-z])(lon|lng|long|longitude)([^a-z]|$)`)
	wktRe    = regexp.MustCompile(`(?i)^\s*(SRID=\d+;)?\s*(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*(Z|M|ZM)?\s*(\(|EMPTY)`)
	jwtRe    = regexp.MustCompile(`eyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]*`)
	pemRe    = regexp.MustCompile(`-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----`)
	awsRe    = regexp.MustCompile(`\b(AKIA|ASIA|AGPA|AIDA|AROA|ANPA|ANVA|AIPA)[A-Z2-7]{16}\b`)
	assignRe = regexp.MustCompile(`(?i)\b(password|passwd|pwd|secret|client[_-]?secret|api[_-]?key|apikey|access[_-]?token)\b["']?\s*[=:]\s*["']?[^\s"',;&]{3,}`)
	secretNm = regexp.MustCompile(`(?i)(password|passwd|pwd|secret|api[_-]?key|apikey|private[_-]?key)`)
	tokenNm  = regexp.MustCompile(`(?i)(token|credential|bearer|auth)`)
	emailRe  = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9\-]+(\.[A-Za-z0-9\-]+)*\.[A-Za-z]{2,}`)
	phoneVal = regexp.MustCompile(`^\s*\+?[\d\s().\-]{7,24}\s*$`)
	phoneNm  = regexp.MustCompile(`(?i)(phone|mobile|msisdn|(^|[^a-z])tel([^a-z]|$)|telephone)`)
	dateLike = regexp.MustCompile(`^\s*\d{4}-\d{2}-\d{2}`)
	ibanRe   = regexp.MustCompile(`\b[A-Z]{2}\d{2}[ ]?(?:[A-Z0-9]{4}[ ]?){2,7}[A-Z0-9]{1,4}\b`)
	cardRe   = regexp.MustCompile(`\b(?:\d[ -]?){13,19}\b`)
	ssnRe    = regexp.MustCompile(`\b(\d{3})-(\d{2})-(\d{4})\b`)
	nameBad  = regexp.MustCompile(`[^\p{L}\p{N}_.\[\]$ \-/]`)
)

// leaf classifies one scalar (string or float64) found in field.
func (s *Scanner) leaf(field string, v any) {
	var str string
	var num float64
	isNum := false
	switch x := v.(type) {
	case float64:
		num, isNum = x, true
	case string:
		str = x
		if len(str) > maxValueScan {
			str = str[:maxValueScan]
		}
	default:
		return
	}

	// geo: coordinate columns with in-range numbers, WKT text
	if isNum || str != "" {
		f := num
		ok := isNum
		if !isNum {
			var err error
			f, err = strconv.ParseFloat(strings.TrimSpace(str), 64)
			ok = err == nil
		}
		if ok && !math.IsNaN(f) {
			if (latName.MatchString(field) && f >= -90 && f <= 90) || (lonName.MatchString(field) && f >= -180 && f <= 180) {
				s.hit(Geo, field)
			}
		}
	}
	if isNum {
		return
	}
	if str == "" {
		return
	}
	if wktRe.MatchString(str) {
		s.hit(Geo, field)
	}

	// credentials
	switch {
	case jwtRe.MatchString(str), pemRe.MatchString(str), awsRe.MatchString(str), assignRe.MatchString(str):
		s.hit(Credentials, field)
	case secretNm.MatchString(field) && len(strings.TrimSpace(str)) >= 4:
		s.hit(Credentials, field)
	case tokenNm.MatchString(field) && highEntropyToken(str):
		s.hit(Credentials, field)
	}

	// pii-lite
	if emailRe.MatchString(str) || hasIBAN(str) || hasCard(str) || hasSSN(str) {
		s.hit(PII, field)
	} else if isPhone(field, str) {
		s.hit(PII, field)
	}
}

func isPhone(field, v string) bool {
	if dateLike.MatchString(v) || !phoneVal.MatchString(v) {
		return false
	}
	digits := 0
	for _, r := range v {
		if r >= '0' && r <= '9' {
			digits++
		}
	}
	if phoneNm.MatchString(field) {
		return digits >= 7 && digits <= 15
	}
	t := strings.TrimSpace(v)
	formatted := strings.HasPrefix(t, "+") || strings.ContainsAny(t, " ()-.")
	return digits >= 10 && digits <= 15 && formatted
}

// highEntropyToken is a long, space-free string with random-looking characters.
func highEntropyToken(v string) bool {
	v = strings.TrimSpace(v)
	if len(v) < 20 || len(v) > maxValueScan || strings.ContainsAny(v, " \t\n") {
		return false
	}
	counts := map[rune]int{}
	n := 0
	for _, r := range v {
		counts[r]++
		n++
	}
	h := 0.0
	for _, c := range counts {
		p := float64(c) / float64(n)
		h -= p * math.Log2(p)
	}
	return h >= 3.5
}

// safeName makes a field name safe to store: capped, restricted charset, and replaced when the name itself matches a
// credential pattern (an attacker could put a secret in a key).
func safeName(n string) string {
	if len(n) > maxFieldName {
		n = n[:maxFieldName]
	}
	if jwtRe.MatchString(n) || pemRe.MatchString(n) || awsRe.MatchString(n) || assignRe.MatchString(n) || emailRe.MatchString(n) || highEntropyToken(n) {
		return Redacted
	}
	n = nameBad.ReplaceAllString(n, "?")
	if !strings.ContainsFunc(n, func(r rune) bool { return r != '?' }) {
		return Redacted
	}
	return n
}

// hasIBAN requires a valid mod-97 checksum, which removes almost all false positives.
func hasIBAN(v string) bool {
	for _, m := range ibanRe.FindAllString(v, 4) {
		c := strings.ReplaceAll(m, " ", "")
		if len(c) < 15 || len(c) > 34 {
			continue
		}
		c = c[4:] + c[:4]
		rem := 0
		ok := true
		for _, r := range c {
			var d int
			switch {
			case r >= '0' && r <= '9':
				d = int(r - '0')
				rem = (rem*10 + d) % 97
			case r >= 'A' && r <= 'Z':
				d = int(r-'A') + 10
				rem = (rem*100 + d) % 97
			default:
				ok = false
			}
		}
		if ok && rem == 1 {
			return true
		}
	}
	return false
}

// hasCard finds 13-19 digit runs that pass the Luhn check.
func hasCard(v string) bool {
	for _, m := range cardRe.FindAllString(v, 4) {
		var ds []int
		for _, r := range m {
			if r >= '0' && r <= '9' {
				ds = append(ds, int(r-'0'))
			}
		}
		if len(ds) < 13 || len(ds) > 19 {
			continue
		}
		sum := 0
		for i := range ds {
			d := ds[len(ds)-1-i]
			if i%2 == 1 {
				if d *= 2; d > 9 {
					d -= 9
				}
			}
			sum += d
		}
		if sum%10 == 0 {
			return true
		}
	}
	return false
}

// hasSSN matches US SSN shape with the invalid area/group/serial ranges excluded.
func hasSSN(v string) bool {
	for _, m := range ssnRe.FindAllStringSubmatch(v, 4) {
		if m[1] != "000" && m[1] != "666" && m[1][0] != '9' && m[2] != "00" && m[3] != "0000" {
			return true
		}
	}
	return false
}
