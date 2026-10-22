// Package metadata extracts a small, bounded set of document metadata.
//
// It is deliberately value-safe for the ingestion path: metadata values are
// returned to the caller, never logged or persisted by this package. Callers
// that only need classification should use Summary.Labels and discard Values.
package metadata

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Coordinate struct{ Latitude, Longitude float64 }

type Result struct {
	Format      string            `json:"format"`
	Values      map[string]string `json:"values,omitempty"`
	Coordinates []Coordinate      `json:"coordinates,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Truncated   bool              `json:"truncated,omitempty"`
}

type Limits struct {
	MaxBytes                  int64
	MaxValues, MaxCoordinates int
}

const (
	safeMaxBytes       int64 = 8 << 20
	safeMaxValues            = 32
	safeMaxCoordinates       = 1000
)

func SafeLimits() Limits {
	return Limits{MaxBytes: safeMaxBytes, MaxValues: safeMaxValues, MaxCoordinates: safeMaxCoordinates}
}

var ErrUnsupported = errors.New("metadata: unsupported document")

func normalize(l Limits) Limits {
	d := SafeLimits()
	if l.MaxBytes <= 0 {
		l.MaxBytes = d.MaxBytes
	} else if l.MaxBytes > d.MaxBytes {
		l.MaxBytes = d.MaxBytes
	}
	if l.MaxValues <= 0 {
		l.MaxValues = d.MaxValues
	} else if l.MaxValues > d.MaxValues {
		l.MaxValues = d.MaxValues
	}
	if l.MaxCoordinates <= 0 {
		l.MaxCoordinates = d.MaxCoordinates
	} else if l.MaxCoordinates > d.MaxCoordinates {
		l.MaxCoordinates = d.MaxCoordinates
	}
	return l
}

func Extract(ctx context.Context, r io.Reader, contentType string, l Limits) (Result, error) {
	l = normalize(l)
	b, truncated, err := readBounded(ctx, r, l.MaxBytes)
	if err != nil {
		return Result{}, err
	}
	res := Result{Values: map[string]string{}, Truncated: truncated}
	mt, _, _ := strings.Cut(strings.ToLower(contentType), ";")
	switch {
	case bytes.HasPrefix(b, []byte("%PDF-")) || mt == "application/pdf":
		res.Format = "pdf"
		addTag(&res, "document", l)
		addTag(&res, "format.pdf", l)
		extractPDF(&res, b, l)
	case bytes.HasPrefix(b, []byte("\x89PNG\r\n\x1a\n")) || mt == "image/png":
		res.Format = "png"
		addTag(&res, "image", l)
		addTag(&res, "format.png", l)
		extractPNG(&res, b, l)
	case bytes.HasPrefix(b, []byte("\xff\xd8\xff")) || strings.HasPrefix(mt, "image/jpeg"):
		res.Format = "jpeg"
		addTag(&res, "image", l)
		addTag(&res, "format.jpeg", l)
		extractJPEG(&res, b, l)
	case mt == "application/json" || mt == "application/x-ndjson" || mt == "text/json" || looksJSON(b):
		res.Format = "json"
		addTag(&res, "structured_data", l)
		extractJSON(&res, b, l)
	default:
		return Result{}, ErrUnsupported
	}
	coordinatesFromValues(&res, l)
	if len(res.Values) == 0 && len(res.Coordinates) == 0 && res.Format == "" {
		return Result{}, ErrUnsupported
	}
	return res, nil
}

func readBounded(ctx context.Context, r io.Reader, max int64) ([]byte, bool, error) {
	var b bytes.Buffer
	br := bufio.NewReader(r)
	buf := make([]byte, 32<<10)
	for int64(b.Len()) <= max {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		n, err := br.Read(buf)
		if n > 0 {
			remain := max + 1 - int64(b.Len())
			if int64(n) > remain {
				n = int(remain)
			}
			_, _ = b.Write(buf[:n])
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, false, err
		}
		if int64(b.Len()) > max {
			return b.Bytes()[:max], true, nil
		}
	}
	return b.Bytes(), false, nil
}

func looksJSON(b []byte) bool {
	b = bytes.TrimSpace(b)
	return len(b) > 0 && (b[0] == '{' || b[0] == '[')
}

func addTag(r *Result, tag string, l Limits) {
	for _, existing := range r.Tags {
		if existing == tag {
			return
		}
	}
	if len(r.Tags) >= l.MaxValues {
		r.Truncated = true
		return
	}
	r.Tags = append(r.Tags, tag)
	sort.Strings(r.Tags)
}

var latKey = regexp.MustCompile(`(?i)^(lat|latitude|y)$`)
var lonKey = regexp.MustCompile(`(?i)^(lon|lng|longitude|x)$`)
var wkt = regexp.MustCompile(`(?i)\bPOINT\s*\(\s*(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)`)

func extractJSON(res *Result, b []byte, l Limits) {
	var v any
	if json.Unmarshal(bytes.TrimSpace(b), &v) != nil { // NDJSON: inspect the first bounded records.
		for _, line := range bytes.Split(b, []byte{'\n'}) {
			var x any
			if json.Unmarshal(bytes.TrimSpace(line), &x) == nil {
				walkJSON(res, "", x, l, 0)
			}
		}
		return
	}
	walkJSON(res, "", v, l, 0)
}

func walkJSON(res *Result, path string, v any, l Limits, depth int) {
	if depth > 16 || len(res.Coordinates) >= l.MaxCoordinates {
		res.Truncated = true
		return
	}
	switch x := v.(type) {
	case map[string]any:
		var lat, lon *float64
		for k, c := range x {
			if n, ok := number(c); ok {
				if latKey.MatchString(k) && n >= -90 && n <= 90 {
					lat = &n
				}
				if lonKey.MatchString(k) && n >= -180 && n <= 180 {
					lon = &n
				}
			}
		}
		if lat != nil && lon != nil {
			addTag(res, "contains_coordinates", l)
			res.Coordinates = append(res.Coordinates, Coordinate{*lat, *lon})
		}
		if t, _ := x["type"].(string); t == "Point" {
			addTag(res, "geojson", l)
			if a, ok := x["coordinates"].([]any); ok && len(a) >= 2 {
				if lo, ok1 := number(a[0]); ok1 {
					if la, ok2 := number(a[1]); ok2 && lo >= -180 && lo <= 180 && la >= -90 && la <= 90 {
						addTag(res, "contains_coordinates", l)
						res.Coordinates = append(res.Coordinates, Coordinate{la, lo})
					}
				}
			}
		}
		for k, c := range x {
			walkJSON(res, join(path, k), c, l, depth+1)
		}
	case []any:
		for _, c := range x {
			walkJSON(res, path, c, l, depth+1)
		}
	case string:
		if m := wkt.FindStringSubmatch(x); len(m) == 3 {
			lo, e1 := strconv.ParseFloat(m[1], 64)
			la, e2 := strconv.ParseFloat(m[2], 64)
			if e1 == nil && e2 == nil && lo >= -180 && lo <= 180 && la >= -90 && la <= 90 {
				addTag(res, "contains_coordinates", l)
				res.Coordinates = append(res.Coordinates, Coordinate{la, lo})
			}
		}
	}
}
func join(a, b string) string {
	if a == "" {
		return b
	}
	return a + "." + b
}
func number(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case json.Number:
		x, e := n.Float64()
		return x, e == nil
	}
	return 0, false
}

func addValue(r *Result, k, v string, l Limits) {
	if len(r.Values) >= l.MaxValues || len(k) > 64 || len(v) > 256 {
		r.Truncated = true
		return
	}
	r.Values[k] = v
}

var pdfValue = regexp.MustCompile(`/([A-Za-z]+)\s*\(([^()]*)\)`)

func extractPDF(r *Result, b []byte, l Limits) {
	for _, m := range pdfValue.FindAllSubmatch(b, -1) {
		k := string(m[1])
		switch k {
		case "Title", "Author", "Creator", "Producer", "Subject", "Keywords", "CreationDate", "ModDate", "GPSLatitude", "GPSLongitude":
			addValue(r, "pdf."+strings.ToLower(k), string(m[2]), l)
			addTag(r, "metadata.pdf_info", l)
		}
	}
	extractXMP(r, b, l)
}
func extractXMP(r *Result, b []byte, l Limits) {
	addTag(r, "metadata.xmp", l)
	s := string(b)
	for _, k := range []string{"GPSLatitude", "GPSLongitude", "dc:title", "xmp:CreatorTool", "xmp:CreateDate", "xmp:ModifyDate"} {
		re := regexp.MustCompile(`(?is)(?:[A-Za-z0-9_:.-]+:)?` + regexp.QuoteMeta(k) + `[^>]*>([^<]+)`)
		if m := re.FindStringSubmatch(s); len(m) == 2 {
			addValue(r, "xmp."+strings.ToLower(k), strings.TrimSpace(m[1]), l)
		}
	}
}

func extractPNG(r *Result, b []byte, l Limits) {
	if len(b) < 8 {
		return
	}
	p := 8
	for p+12 <= len(b) {
		n := int(binary.BigEndian.Uint32(b[p : p+4]))
		if n < 0 || p+12+n > len(b) {
			r.Truncated = true
			return
		}
		typ := string(b[p+4 : p+8])
		data := b[p+8 : p+8+n]
		if typ == "tEXt" {
			addTag(r, "metadata.png_text", l)
			if i := bytes.IndexByte(data, 0); i > 0 {
				addValue(r, "png."+strings.ToLower(string(data[:i])), string(data[i+1:]), l)
			}
		}
		p += 12 + n
	}
}
func extractJPEG(r *Result, b []byte, l Limits) { // JPEG APP1 XMP is text; EXIF GPS is intentionally handled through the same value-safe XMP keys.
	for p := 2; p+4 <= len(b) && b[p] == 0xff; {
		marker := b[p+1]
		if marker == 0xda || marker == 0xd9 {
			break
		}
		n := int(binary.BigEndian.Uint16(b[p+2 : p+4]))
		if n < 2 || p+2+n > len(b) {
			r.Truncated = true
			return
		}
		data := b[p+4 : p+2+n]
		if bytes.Contains(data, []byte("http://ns.adobe.com/xap/1.0/")) {
			addTag(r, "metadata.xmp", l)
			extractXMP(r, data, l)
		}
		if bytes.HasPrefix(data, []byte("Exif\x00\x00")) {
			addTag(r, "metadata.exif", l)
			extractEXIF(r, data[6:], l)
		}
		p += 2 + n
		_ = marker
	}
}

// extractEXIF reads only the GPS IFD from a TIFF payload. It supports the two
// TIFF byte orders and rational GPS degrees/minutes/seconds values. No other
// EXIF values are copied because camera metadata can contain sensitive device
// and user information.
func extractEXIF(r *Result, b []byte, l Limits) {
	if len(b) < 8 || (string(b[:2]) != "II" && string(b[:2]) != "MM") {
		return
	}
	var order binary.ByteOrder = binary.LittleEndian
	if string(b[:2]) == "MM" {
		order = binary.BigEndian
	}
	if order.Uint16(b[2:4]) != 42 {
		return
	}
	first := int(order.Uint32(b[4:8]))
	if gps, ok := tiffTag(b, order, first, 0x8825); ok {
		parseGPSIFD(r, b, order, int(gps), l)
	}
}

func tiffTag(b []byte, order binary.ByteOrder, off, wanted int) (uint32, bool) {
	if off < 0 || off+2 > len(b) {
		return 0, false
	}
	n := int(order.Uint16(b[off : off+2]))
	for i := 0; i < n && off+2+12*(i+1) <= len(b); i++ {
		e := off + 2 + 12*i
		if int(order.Uint16(b[e:e+2])) == wanted {
			return order.Uint32(b[e+8 : e+12]), true
		}
	}
	return 0, false
}

func parseGPSIFD(r *Result, b []byte, order binary.ByteOrder, off int, l Limits) {
	if off < 0 || off+2 > len(b) {
		return
	}
	n := int(order.Uint16(b[off : off+2]))
	var latRef, lonRef string
	var lat, lon [3]float64
	var haveLat, haveLon bool
	for i := 0; i < n && off+2+12*(i+1) <= len(b); i++ {
		e := off + 2 + 12*i
		tag, typ := int(order.Uint16(b[e:e+2])), int(order.Uint16(b[e+2:e+4]))
		count := order.Uint32(b[e+4 : e+8])
		if typ == 2 && count > 0 && count <= 8 {
			v := tiffASCII(b, order, uint32(e+8), count)
			switch tag {
			case 1:
				latRef = v
			case 3:
				lonRef = v
			}
		}
		if typ == 5 && count >= 3 && count <= 3 {
			v, ok := tiffRationals(b, order, uint32(e+8), count)
			if ok {
				switch tag {
				case 2:
					copy(lat[:], v)
					haveLat = true
				case 4:
					copy(lon[:], v)
					haveLon = true
				}
			}
		}
	}
	if !haveLat || !haveLon || len(r.Values) >= l.MaxValues {
		return
	}
	if strings.EqualFold(latRef, "S") {
		lat[0] = -lat[0]
	}
	if strings.EqualFold(lonRef, "W") {
		lon[0] = -lon[0]
	}
	latitude := lat[0] + lat[1]/60 + lat[2]/3600
	longitude := lon[0] + lon[1]/60 + lon[2]/3600
	if latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180 {
		return
	}
	addValue(r, "exif.gpslatitude", strconv.FormatFloat(latitude, 'f', 8, 64), l)
	addValue(r, "exif.gpslongitude", strconv.FormatFloat(longitude, 'f', 8, 64), l)
	addTag(r, "contains_gps", l)
}

func tiffASCII(b []byte, order binary.ByteOrder, field, countPos uint32) string {
	count := countPos
	var off int
	if count <= 4 {
		off = int(field)
	} else {
		if field > uint32(len(b)) || len(b)-int(field) < 4 {
			return ""
		}
		off = int(order.Uint32(b[field : field+4]))
	}
	if off < 0 || off > len(b) || uint64(count) > uint64(len(b)-off) {
		return ""
	}
	return strings.TrimRight(string(b[off:off+int(count)]), "\x00")
}

func tiffRationals(b []byte, order binary.ByteOrder, field uint32, count uint32) ([]float64, bool) {
	if count != 3 || field > uint32(len(b)) || len(b)-int(field) < 4 {
		return nil, false
	}
	off := int(order.Uint32(b[field : field+4]))
	if off < 0 || off > len(b) || len(b)-off < 24 {
		return nil, false
	}
	out := make([]float64, count)
	for i := range out {
		n := order.Uint32(b[off+8*i : off+8*i+4])
		d := order.Uint32(b[off+8*i+4 : off+8*i+8])
		if d == 0 {
			return nil, false
		}
		out[i] = float64(n) / float64(d)
	}
	return out, true
}

func coordinatesFromValues(r *Result, l Limits) {
	var lat, lon *float64
	for k, v := range r.Values {
		k = strings.ToLower(k)
		if strings.HasSuffix(k, "gpslatitude") || strings.HasSuffix(k, "latitude") {
			if n, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
				lat = &n
			}
		}
		if strings.HasSuffix(k, "gpslongitude") || strings.HasSuffix(k, "longitude") {
			if n, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
				lon = &n
			}
		}
	}
	if lat != nil && lon != nil && *lat >= -90 && *lat <= 90 && *lon >= -180 && *lon <= 180 && len(r.Coordinates) < l.MaxCoordinates {
		r.Coordinates = append(r.Coordinates, Coordinate{Latitude: *lat, Longitude: *lon})
		addTag(r, "contains_gps", l)
	}
}

// Validate keeps coordinate results usable by policy code without accepting NaN/Inf.
func (c Coordinate) Valid() bool {
	return c.Latitude >= -90 && c.Latitude <= 90 && c.Longitude >= -180 && c.Longitude <= 180 && !isNaN(c.Latitude) && !isNaN(c.Longitude)
}
func isNaN(v float64) bool { return v != v }
