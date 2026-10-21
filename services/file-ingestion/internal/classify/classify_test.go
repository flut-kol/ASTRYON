package classify

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/astryon/file-ingestion/internal/parse"
)

func scanJSON(t *testing.T, doc string) Labels {
	t.Helper()
	s := NewScanner()
	s.ScanJSON([]byte(doc))
	return s.Take()
}

func TestGeoDetectors(t *testing.T) {
	for name, doc := range map[string]string{
		"lat/lon columns": `{"lat": 48.1, "lon": 11.5}`,
		"string coords":   `{"Latitude": "48.1", "longitude": "-11.5"}`,
		"wkt":             `{"shape": "POLYGON ((0 0, 1 0, 1 1, 0 0))"}`,
		"geojson point":   `{"loc": {"type": "Point", "coordinates": [11.5, 48.1]}}`,
		"geojson feature": `{"type": "Feature", "geometry": null, "properties": {}}`,
	} {
		if !scanJSON(t, doc).Has(Geo) {
			t.Errorf("%s: not detected", name)
		}
	}
	for name, doc := range map[string]string{
		"out of range": `{"lat": 148.1, "lon": 211.5}`,
		"not a coord":  `{"platform": 12, "type": "Point"}`,
		"plain":        `{"a": "POINT of sale"}`,
	} {
		if scanJSON(t, doc).Has(Geo) {
			t.Errorf("%s: false positive", name)
		}
	}
}

func TestCredentialDetectors(t *testing.T) {
	jwt := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc123_-sig"
	for name, doc := range map[string]string{
		"jwt":          `{"note": "` + jwt + `"}`,
		"pem":          `{"k": "-----BEGIN RSA PRIVATE KEY-----\nMIIE"}`,
		"aws":          `{"k": "id AKIAIOSFODNN7EXAMPLE here"}`,
		"assignment":   `{"s": "db password=hunter2x"}`,
		"api_key text": `{"s": "api_key: abcd1234"}`,
		"secret field": `{"client_secret": "s3cr3t-value"}`,
		"entropy":      `{"auth_token": "Zk3Qx9LmP2vT8aWc5RnY1uHb7"}`,
	} {
		if !scanJSON(t, doc).Has(Credentials) {
			t.Errorf("%s: not detected", name)
		}
	}
	for name, doc := range map[string]string{
		"plain":           `{"msg": "hello world"}`,
		"low entropy tok": `{"token": "aaaaaaaaaaaaaaaaaaaaaaaa"}`,
		"empty password":  `{"password": ""}`,
	} {
		if scanJSON(t, doc).Has(Credentials) {
			t.Errorf("%s: false positive", name)
		}
	}
}

func TestPIIDetectors(t *testing.T) {
	for name, doc := range map[string]string{
		"email":  `{"c": "write to jane.doe@example.com"}`,
		"phone":  `{"c": "+49 151 2345 6789"}`,
		"phone2": `{"mobile": "5551234567"}`,
		"iban":   `{"acct": "DE89 3704 0044 0532 0130 00"}`,
		"card":   `{"pan": "4111 1111 1111 1111"}`,
		"ssn":    `{"id": "123-45-6789"}`,
	} {
		if !scanJSON(t, doc).Has(PII) {
			t.Errorf("%s: not detected", name)
		}
	}
	for name, doc := range map[string]string{
		"date":         `{"d": "2024-01-02 10:30:00"}`,
		"bad luhn":     `{"pan": "4111 1111 1111 1112"}`,
		"bad iban":     `{"acct": "DE00 3704 0044 0532 0130 00"}`,
		"number phone": `{"n": 5551234567}`,
		"plain digits": `{"n": "5551234567"}`,
		"invalid ssn":  `{"id": "000-12-3456"}`,
	} {
		if scanJSON(t, doc).Has(PII) {
			t.Errorf("%s: false positive", name)
		}
	}
}

func TestCSVThroughBoundedParser(t *testing.T) {
	csvData := "name,email,lat,lon\nann,ann@example.com,10.5,20.5\nbob,bob@example.com,11,21\n"
	s := NewScanner()
	var header []string
	err := parse.CSV(strings.NewReader(csvData), 1<<10, func(r parse.CSVRecord) error {
		if r.Number == 1 {
			header = r.Fields
			return nil
		}
		s.ScanCSV(header, r.Fields)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	l := s.Take()
	if l[PII].Count != 2 || l[Geo].Count != 4 || l[PII].Fields[0] != "email" {
		t.Fatalf("%+v", l)
	}
}

func TestDetectorSelectionAndMerge(t *testing.T) {
	s := NewScannerFor(Geo)
	s.ScanJSON([]byte(`{"lat": 1, "email": "a@b.co", "password": "abcdefgh"}`))
	l := s.Take()
	if !l.Has(Geo) || l.Has(PII) || l.Has(Credentials) {
		t.Fatalf("%+v", l)
	}
	a := Labels{Geo: {Count: 1, Fields: []string{"lat"}}}
	a.Merge(Labels{Geo: {Count: 2, Fields: []string{"lon", "lat"}}})
	if a[Geo].Count != 3 || len(a[Geo].Fields) != 2 {
		t.Fatalf("%+v", a)
	}
}

func TestFieldNamesAreBoundedAndNeverCarrySecrets(t *testing.T) {
	long := strings.Repeat("x", 200)
	doc := map[string]any{"AKIAIOSFODNN7EXAMPLE": "a@b.co", long: "a@b.co", "ok.name": "a@b.co", "ключ": "a@b.co"}
	raw, _ := json.Marshal(doc)
	l := scanJSON(t, string(raw))
	for _, f := range l[PII].Fields {
		if strings.Contains(f, "AKIA") || len(f) > maxFieldName {
			t.Fatalf("bad field name %q", f)
		}
	}
	if !contains(l[PII].Fields, Redacted) || !contains(l[PII].Fields, "ok.name") {
		t.Fatalf("%+v", l[PII].Fields)
	}
}

func TestMatchedValuesNeverInLabels(t *testing.T) {
	secrets := []string{"hunter2-super", "jane.doe@example.com", "AKIAIOSFODNN7EXAMPLE", "4111111111111111", "123-45-6789", "+49 151 2345 6789"}
	doc := `{"password":"hunter2-super","contact":"jane.doe@example.com","k":"AKIAIOSFODNN7EXAMPLE","pan":"4111111111111111","ssn":"123-45-6789","phone":"+49 151 2345 6789","lat":10.1}`
	l := scanJSON(t, doc)
	out, _ := json.Marshal(l)
	for _, s := range secrets {
		if bytes.Contains(out, []byte(s)) {
			t.Fatalf("value %q leaked: %s", s, out)
		}
	}
	if len(l) != 3 {
		t.Fatalf("%s", out)
	}
}

func TestPolicyParsingAndResolution(t *testing.T) {
	if p, err := ParsePolicy(""); err != nil || p != Quarantine {
		t.Fatal(p, err)
	}
	if _, err := ParsePolicy("nope"); err == nil {
		t.Fatal("accepted bad policy")
	}
	ov, err := ParseOverrides("t1/s1=block, t2=allow")
	if err != nil {
		t.Fatal(err)
	}
	ps := PolicySet{Default: Quarantine, Overrides: ov}
	if ps.For("t1", "s1") != Block || ps.For("t1", "other") != Quarantine || ps.For("t2", "x") != Allow || (PolicySet{}).For("a", "b") != Quarantine {
		t.Fatal("resolution")
	}
	if _, err := ParseOverrides("garbage"); err == nil {
		t.Fatal("accepted garbage")
	}
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}
