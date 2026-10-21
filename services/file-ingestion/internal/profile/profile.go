// Package profile is the static, code-registered table of source-type processing profiles. A profile decides which
// formats an upload may use, which classifier detectors run, how credentials / geo / PII findings are handled, the
// sensitivity default and floor, required tags, size/row/column/line limits, required fields and the sink gating.
// There are no user-supplied rules: clients only pick a name; an unknown name resolves to "unknown" (strictest detectors).
//
// Enforced today: formats/extensions, detectors, the three policies, sensitivity floor, required tags, limits,
// required fields + on-failure behaviour, sink gating. Declared metadata only (stored/exposed, NOT yet enforced):
// ArchiveNestingLimit, Encoding, RetentionClass, RequiredReadScope, ExportAllowed, MaskPII/TokenizePII, DedupKey,
// Audit/AlertOnFindings. See the README.
package profile

import (
	"encoding/json"
	"errors"
	"slices"
	"sort"
	"strings"

	"github.com/astryon/file-ingestion/internal/classify"
)

const DefaultName = "unknown"

// Codes recorded when a profile holds a job back (see docs/errors/api-error-catalog.md).
const (
	CodePII         = "PII_DETECTED"
	CodeGeo         = "GEO_DETECTED"
	CodeGeoRequired = "GEO_COORDINATES_REQUIRED"
	CodeRowLimit    = "ROW_LIMIT_EXCEEDED"
	CodeSchema      = "SCHEMA_VIOLATION"
)

// Sensitivity levels in increasing order.
var levels = []string{"public", "internal", "restricted"}

// ValidSensitivity reports whether s is a known level.
func ValidSensitivity(s string) bool { return slices.Contains(levels, s) }

func rank(s string) int { return slices.Index(levels, s) }

type OnInvalid string

const (
	DropRow  OnInvalid = "drop_row"  // invalid / schema-violating rows go to processing_errors, the file continues
	FailFile OnInvalid = "fail_file" // the first such row fails the whole file before anything reaches the sink
)

type Profile struct {
	Name string

	// Format (enforced at upload creation).
	Formats    []string // MIME types
	Extensions []string // lower-case, with dot
	// Declared only: ArchiveNestingLimit 0 = archives not accepted; Encoding is the expected text encoding.
	ArchiveNestingLimit int
	Encoding            string

	// Detectors that run (empty = all).
	Detectors []classify.Label
	// Policies. "" credentials = env default (FILE_CREDENTIALS_POLICY); "" geo/pii = tag. Values: allow|tag|quarantine|block.
	Credentials, Geo, PII classify.Policy
	RequireGeo            bool // block with GEO_COORDINATES_REQUIRED unless valid coordinates/geometries were found

	// Sensitivity.
	DefaultSensitivity string
	SensitivityFloor   string   // cannot be lowered by the client; "" = none
	RequiredTags       []string // tag keys the upload must carry

	// Limits (0 = no profile limit; service limits still apply).
	MaxBytes, MaxRecords int64
	MaxColumns           int // top-level fields per record
	MaxLineBytes         int // per-record bytes

	// Parser. Only "ndjson" is wired in the worker.
	Parser         string
	RequiredFields []string // top-level fields every record must have
	OnInvalid      OnInvalid

	// Handling. NoSink is enforced (file is classified, nothing is written); the rest is declared.
	NoSink               bool
	MaskPII, TokenizePII bool
	DedupKey             string

	// Declared metadata (not enforced yet).
	RetentionClass    string
	RequiredReadScope string
	ExportAllowed     bool
	Audit             string // basic|detailed
	AlertOnFindings   bool
}

var (
	ndjson   = []string{"application/x-ndjson"}
	both     = []string{"application/x-ndjson", "text/csv"}
	extND    = []string{".ndjson", ".jsonl", ".json"}
	extBoth  = []string{".ndjson", ".jsonl", ".json", ".csv"}
	credPII  = []classify.Label{classify.Credentials, classify.PII}
	credGeo  = []classify.Label{classify.Credentials, classify.Geo}
	credOnly = []classify.Label{classify.Credentials}
)

func base(name string, f, e []string) Profile {
	return Profile{Name: name, Formats: f, Extensions: e, Encoding: "utf-8", DefaultSensitivity: "internal", Parser: "ndjson",
		OnInvalid: DropRow, DedupKey: "fingerprint+offset", RetentionClass: "standard", RequiredReadScope: "astryon:data:read",
		ExportAllowed: true, Audit: "basic"}
}

var registry = func() map[string]Profile {
	m := map[string]Profile{}
	add := func(p Profile) { m[p.Name] = p }

	add(func() Profile {
		p := base("unknown", both, extBoth)
		// credentials: "" = env default (FILE_CREDENTIALS_POLICY, quarantine unless configured); all detectors run
		p.Audit = "detailed"
		return p
	}())
	add(func() Profile {
		p := base("telemetry", ndjson, extND)
		p.Detectors = credGeo
		p.MaxRecords = 100_000_000
		return p
	}())
	add(func() Profile {
		p := base("geo", both, extBoth)
		p.Detectors = credGeo
		p.RequireGeo = true
		p.OnInvalid = FailFile
		return p
	}())
	add(func() Profile {
		p := base("report", both, extBoth)
		p.Detectors = credPII
		p.Credentials = classify.Quarantine // free text allowed, credentials strictly held back
		return p
	}())
	add(func() Profile { p := base("export", both, extBoth); p.DefaultSensitivity = "restricted"; return p }())
	add(func() Profile {
		p := base("config", ndjson, extND)
		p.Detectors = credOnly
		p.Credentials = classify.Quarantine
		p.SensitivityFloor, p.DefaultSensitivity = "restricted", "restricted"
		p.MaxBytes, p.MaxRecords, p.MaxColumns = 10<<20, 100_000, 1000
		return p
	}())
	add(func() Profile {
		p := base("log", ndjson, extND)
		p.Detectors = credPII
		p.Credentials = classify.Quarantine
		p.MaxLineBytes = 64 << 10
		p.RetentionClass = "short"
		return p
	}())
	add(func() Profile {
		p := base("credentials_bundle", ndjson, extND)
		p.Detectors = credOnly
		p.Credentials = classify.Block
		p.SensitivityFloor, p.DefaultSensitivity = "restricted", "restricted"
		p.NoSink, p.ExportAllowed, p.Audit, p.AlertOnFindings = true, false, "detailed", true
		p.RequiredReadScope = "astryon:secrets:read"
		return p
	}())
	strict := func(name string) Profile {
		p := base(name, both, extBoth)
		p.Credentials, p.PII, p.Geo = classify.Block, classify.Quarantine, classify.Tag
		p.SensitivityFloor, p.DefaultSensitivity = "restricted", "restricted"
		p.RequiredTags = []string{"purpose"}
		p.MaxColumns, p.MaxLineBytes = 500, 256<<10
		p.OnInvalid = FailFile
		p.MaskPII, p.TokenizePII = true, true
		p.ExportAllowed, p.Audit, p.AlertOnFindings = false, "detailed", true
		p.RetentionClass, p.RequiredReadScope = "regulated", "astryon:pii:read"
		return p
	}
	add(strict("personal_data"))
	add(strict("financial"))
	add(func() Profile {
		p := base("media_meta", ndjson, extND)
		p.Detectors = []classify.Label{classify.Geo, classify.Credentials, classify.PII}
		return p
	}())
	add(func() Profile {
		p := base("reference", both, extBoth)
		p.Detectors = credOnly
		p.Credentials = classify.Quarantine
		p.DefaultSensitivity = "public"
		p.MaxRecords = 10_000_000
		p.RetentionClass = "long"
		return p
	}())
	return m
}()

// Names lists the registered profiles sorted (OpenAPI enum / docs).
func Names() []string {
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Select returns the profile for a client-supplied source type; unknown or empty -> "unknown".
func Select(sourceType string) Profile {
	if p, ok := registry[sourceType]; ok {
		return p
	}
	return registry[DefaultName]
}

// AllowsFormat checks the MIME type and the file extension.
func (p Profile) AllowsFormat(contentType, filename string) bool {
	if !slices.Contains(p.Formats, contentType) {
		return false
	}
	i := strings.LastIndex(filename, ".")
	return i >= 0 && slices.Contains(p.Extensions, strings.ToLower(filename[i:]))
}

// Sensitivity resolves the requested level ("" = profile default) against the floor. It reports false for an invalid level.
func (p Profile) Sensitivity(requested string) (string, bool) {
	if requested == "" {
		requested = p.DefaultSensitivity
	}
	if !ValidSensitivity(requested) {
		return "", false
	}
	if p.SensitivityFloor != "" && rank(requested) < rank(p.SensitivityFloor) {
		requested = p.SensitivityFloor
	}
	return requested, true
}

// MissingTag returns the first required tag key absent from tags ("" when complete).
func (p Profile) MissingTag(tags map[string]string) string {
	for _, k := range p.RequiredTags {
		if _, ok := tags[k]; !ok {
			return k
		}
	}
	return ""
}

// NewScanner builds a scanner running only this profile's detectors.
func (p Profile) NewScanner() *classify.Scanner { return classify.NewScannerFor(p.Detectors...) }

func (p Profile) geoPolicy() classify.Policy {
	if p.Geo == "" {
		return classify.Tag
	}
	return p.Geo
}

func (p Profile) piiPolicy() classify.Policy {
	if p.PII == "" {
		return classify.Tag
	}
	return p.PII
}

func holds(pol classify.Policy) bool { return pol == classify.Quarantine || pol == classify.Block }

// Gates reports whether the final labels or records can change the job's fate, i.e. the whole file must be checked
// before any record reaches the sink.
func (p Profile) Gates(cred classify.Policy) bool {
	return holds(cred) || holds(p.geoPolicy()) || holds(p.piiPolicy()) || p.RequireGeo ||
		p.OnInvalid == FailFile || p.NoSink
}

var ErrSchema = errors.New("record violates the source profile schema")

// MaxRecord is the per-record byte cap: the smaller of the worker cap and the profile's line limit.
func (p Profile) MaxRecord(workerCap int) int {
	if p.MaxLineBytes > 0 && p.MaxLineBytes < workerCap {
		return p.MaxLineBytes
	}
	return workerCap
}

// CheckRecord validates one well-formed JSON record against MaxColumns and RequiredFields. Only the top-level keys
// are examined and nothing is retained.
func (p Profile) CheckRecord(raw []byte) error {
	if p.MaxColumns == 0 && len(p.RequiredFields) == 0 {
		return nil
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil {
		return ErrSchema // not an object
	}
	if p.MaxColumns > 0 && len(m) > p.MaxColumns {
		return ErrSchema
	}
	for _, f := range p.RequiredFields {
		if _, ok := m[f]; !ok {
			return ErrSchema
		}
	}
	return nil
}

// EffectiveCredentialsPolicy: explicit tenant/source override > profile > env default.
func (p Profile) EffectiveCredentialsPolicy(set classify.PolicySet, tenant, source string) classify.Policy {
	if pol, ok := set.Explicit(tenant, source); ok {
		return pol
	}
	if p.Credentials != "" {
		return p.Credentials
	}
	return set.For(tenant, source)
}

type Action string

const (
	Proceed    Action = "proceed"
	Quarantine Action = "quarantine"
	Block      Action = "block"
)

// Outcome is the decision for a fully classified file.
type Outcome struct {
	Action Action
	Code   string
	Reason classify.Label
}

// Evaluate applies the three policies to the final labels: credentials first, then PII, then geo (found), then the
// geo requirement. allow and tag both mean "label only".
func (p Profile) Evaluate(cred classify.Policy, l classify.Labels) Outcome {
	for _, c := range []struct {
		label classify.Label
		pol   classify.Policy
		code  string
	}{
		{classify.Credentials, cred, classify.CodeCredentials},
		{classify.PII, p.piiPolicy(), CodePII},
		{classify.Geo, p.geoPolicy(), CodeGeo},
	} {
		if !l.Has(c.label) {
			continue
		}
		switch c.pol {
		case classify.Quarantine:
			return Outcome{Quarantine, c.code, c.label}
		case classify.Block:
			return Outcome{Block, c.code, c.label}
		}
	}
	if p.RequireGeo && !l.Has(classify.Geo) {
		return Outcome{Block, CodeGeoRequired, classify.Geo}
	}
	return Outcome{Action: Proceed}
}
