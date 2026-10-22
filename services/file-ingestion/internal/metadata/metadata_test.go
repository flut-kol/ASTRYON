package metadata

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"
)

func TestJSONCoordinatesAndWKTAreBounded(t *testing.T) {
	r, err := Extract(context.Background(), strings.NewReader(`{"point":{"type":"Point","coordinates":[30.5,50.4]},"shape":"POINT (31 51)"}`), "application/json", Limits{})
	if err != nil || len(r.Coordinates) != 2 || !hasTag(r.Tags, "contains_coordinates") || !hasTag(r.Tags, "geojson") {
		t.Fatalf("%+v %v", r, err)
	}
	if !r.Coordinates[0].Valid() || r.Coordinates[0].Latitude != 50.4 || r.Coordinates[0].Longitude != 30.5 {
		t.Fatalf("bad coordinate: %+v", r.Coordinates[0])
	}
}

func hasTag(tags []string, want string) bool {
	for _, tag := range tags {
		if tag == want {
			return true
		}
	}
	return false
}

func TestPDFAndPNGMetadata(t *testing.T) {
	pdf := "%PDF-1.7 /Title (Quarterly report) /Author (Analyst)"
	r, err := Extract(context.Background(), strings.NewReader(pdf), "application/pdf", Limits{})
	if err != nil || r.Values["pdf.title"] != "Quarterly report" || r.Values["pdf.author"] != "Analyst" || !hasTag(r.Tags, "metadata.pdf_info") {
		t.Fatalf("%+v %v", r, err)
	}
	png := []byte("\x89PNG\r\n\x1a\n")
	data := append([]byte("GPSLatitude"), 0, '5', '0')
	chunk := make([]byte, 12+len(data))
	binary.BigEndian.PutUint32(chunk, uint32(len(data)))
	copy(chunk[4:], []byte("tEXt"))
	copy(chunk[8:], data)
	png = append(png, chunk...)
	r, err = Extract(context.Background(), strings.NewReader(string(png)), "image/png", Limits{})
	if err != nil || r.Values["png.gpslatitude"] != "50" {
		t.Fatalf("%+v %v", r, err)
	}
}

func TestValuesAndCoordinatesCaps(t *testing.T) {
	r, err := Extract(context.Background(), strings.NewReader(`{"a":{"lat":1,"lon":2},"b":{"lat":3,"lon":4}}`), "application/json", Limits{MaxCoordinates: 1, MaxValues: 1})
	if err != nil || len(r.Coordinates) != 1 || !r.Truncated {
		t.Fatalf("%+v %v", r, err)
	}
}

func TestCallerLimitsCannotExceedSafeBounds(t *testing.T) {
	r, err := Extract(context.Background(), strings.NewReader(`{"a":{"lat":1,"lon":2}}`), "application/json", Limits{
		MaxBytes: 1 << 30, MaxValues: 1 << 20, MaxCoordinates: 1 << 20,
	})
	if err != nil || len(r.Coordinates) != 1 {
		t.Fatalf("bounded extraction failed: %+v %v", r, err)
	}
}

func TestMalformedEXIFIsRejectedWithoutPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("malformed EXIF panicked: %v", r)
		}
	}()

	if _, ok := tiffRationals([]byte{1, 2, 3, 4}, binary.LittleEndian, 99, 3); ok {
		t.Fatal("out-of-range rational offset accepted")
	}
	if got := tiffASCII([]byte{1, 2, 3, 4}, binary.LittleEndian, 99, 5); got != "" {
		t.Fatalf("out-of-range ASCII offset returned %q", got)
	}
	// The public extractor must also remain fail-closed on a truncated JPEG.
	b := []byte{0xff, 0xd8, 0xff, 0xe1, 0x00, 0x2a, 'E', 'x', 'i', 'f', 0, 0, 0xff, 0xd9}
	if _, err := Extract(context.Background(), strings.NewReader(string(b)), "image/jpeg", Limits{}); err != nil {
		t.Fatalf("unexpected extraction error: %v", err)
	}
}
