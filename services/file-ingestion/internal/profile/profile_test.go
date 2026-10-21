package profile

import (
	"testing"

	"github.com/astryon/file-ingestion/internal/classify"
)

func labels(ls ...classify.Label) classify.Labels {
	out := classify.Labels{}
	for _, l := range ls {
		out[l] = classify.Finding{Count: 1, Fields: []string{"f"}}
	}
	return out
}

func TestSelectionFallsBackToUnknown(t *testing.T) {
	for _, n := range []string{"telemetry", "geo", "report", "export", "config", "log", "credentials_bundle", "personal_data", "financial", "media_meta", "reference", "unknown"} {
		if Select(n).Name != n {
			t.Errorf("%s not registered", n)
		}
	}
	for _, n := range []string{"", "nope", "TELEMETRY"} {
		if Select(n).Name != "unknown" {
			t.Errorf("%q -> %s", n, Select(n).Name)
		}
	}
	if len(Names()) != 12 {
		t.Fatal(Names())
	}
}

func TestFormatsSensitivityAndTags(t *testing.T) {
	tel := Select("telemetry")
	if !tel.AllowsFormat("application/x-ndjson", "a.JSONL") || tel.AllowsFormat("text/csv", "a.csv") || tel.AllowsFormat("application/x-ndjson", "a.csv") || tel.AllowsFormat("application/x-ndjson", "noext") {
		t.Fatal("format check")
	}
	cfg := Select("config")
	if s, ok := cfg.Sensitivity("public"); !ok || s != "restricted" {
		t.Fatalf("floor: %q %v", s, ok)
	}
	if s, _ := Select("reference").Sensitivity(""); s != "public" {
		t.Fatal(s)
	}
	if _, ok := tel.Sensitivity("secret"); ok {
		t.Fatal("accepted bad level")
	}
	if Select("financial").MissingTag(map[string]string{}) != "purpose" || Select("financial").MissingTag(map[string]string{"purpose": "x"}) != "" {
		t.Fatal("required tags")
	}
	if Select("personal_data").ExportAllowed || Select("financial").ExportAllowed || Select("personal_data").SensitivityFloor != "restricted" {
		t.Fatal("strictest values")
	}
}

func TestPolicyOutcomes(t *testing.T) {
	envQuar, envAllow := classify.Quarantine, classify.Allow
	cases := []struct {
		name   string
		typ    string
		cred   classify.Policy // env default fed in
		l      classify.Labels
		action Action
		code   string
	}{
		{"unknown uses env quarantine", "unknown", envQuar, labels(classify.Credentials), Quarantine, classify.CodeCredentials},
		{"unknown uses env allow", "unknown", envAllow, labels(classify.Credentials), Proceed, ""},
		{"config quarantines even if env allows", "config", envAllow, labels(classify.Credentials), Quarantine, classify.CodeCredentials},
		{"credentials_bundle blocks", "credentials_bundle", envAllow, labels(classify.Credentials), Block, classify.CodeCredentials},
		{"personal_data quarantines pii", "personal_data", envAllow, labels(classify.PII), Quarantine, CodePII},
		{"personal_data blocks credentials", "personal_data", envAllow, labels(classify.Credentials, classify.PII), Block, classify.CodeCredentials},
		{"personal_data tags geo", "personal_data", envAllow, labels(classify.Geo), Proceed, ""},
		{"report tags pii", "report", envAllow, labels(classify.PII), Proceed, ""},
		{"geo requires coordinates", "geo", envAllow, labels(), Block, CodeGeoRequired},
		{"geo with coordinates", "geo", envAllow, labels(classify.Geo), Proceed, ""},
		{"telemetry tags geo", "telemetry", envAllow, labels(classify.Geo), Proceed, ""},
	}
	for _, c := range cases {
		p := Select(c.typ)
		cred := p.EffectiveCredentialsPolicy(classify.PolicySet{Default: c.cred}, "t", "s")
		if out := p.Evaluate(cred, c.l); out.Action != c.action || out.Code != c.code {
			t.Errorf("%s: %v", c.name, out)
		}
	}
}

func TestExplicitOverrideBeatsProfileAndGates(t *testing.T) {
	set := classify.PolicySet{Default: classify.Quarantine, Overrides: map[string]classify.Policy{"t/s": classify.Allow}}
	p := Select("config")
	if p.EffectiveCredentialsPolicy(set, "t", "s") != classify.Allow || p.EffectiveCredentialsPolicy(set, "t", "other") != classify.Quarantine {
		t.Fatal("override order")
	}
	if Select("telemetry").Gates(classify.Allow) || !Select("telemetry").Gates(classify.Quarantine) || !Select("geo").Gates(classify.Allow) || !Select("credentials_bundle").Gates(classify.Allow) {
		t.Fatal("gating")
	}
}

func TestRecordLimits(t *testing.T) {
	p := Select("config")
	if p.CheckRecord([]byte(`{"a":1}`)) != nil {
		t.Fatal("valid record rejected")
	}
	p.RequiredFields = []string{"id"}
	if p.CheckRecord([]byte(`{"a":1}`)) == nil || p.CheckRecord([]byte(`[1]`)) == nil {
		t.Fatal("schema not enforced")
	}
	p.MaxColumns = 1
	if p.CheckRecord([]byte(`{"id":1,"b":2}`)) == nil {
		t.Fatal("columns not enforced")
	}
	if Select("log").MaxRecord(1<<20) != 64<<10 || Select("telemetry").MaxRecord(1<<20) != 1<<20 {
		t.Fatal("line limit")
	}
}
