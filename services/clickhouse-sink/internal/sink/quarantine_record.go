package sink

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	ec "github.com/astryon/event-contracts"
	"github.com/twmb/franz-go/pkg/kgo"
)

const quarantineProducer = "clickhouse-sink"

var reasonCodeRe = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)

// quarantineRecords converts rejected messages to Kafka records. A message
// whose payload carries a UUID tenant_id becomes a strict QuarantineRecordV1 on
// quarTopic. QuarantineRecordV1 requires a tenant, so a message without one
// goes to dlqTopic as a tenantless dead-letter record that carries only the
// source coordinate (never the opaque input, which may hold foreign tenant data).
func quarantineRecords(q []Quarantined, now time.Time, quarTopic, dlqTopic string) ([]*kgo.Record, error) {
	recs := make([]*kgo.Record, 0, len(q))
	for _, x := range q {
		id, err := ec.QuarantineDeviationIDFromOffset(quarantineProducer, x.Msg.Topic, x.Msg.Partition, x.Msg.Offset)
		if err != nil {
			return nil, fmt.Errorf("quarantine id: %w", err)
		}
		reason := reasonCode(x.Code)
		diag := diagnostic(x.Reason)
		part, off := x.Msg.Partition, x.Msg.Offset
		prov := ec.QuarantineProvenanceV1{SourceTopic: x.Msg.Topic, SourcePartition: &part, SourceOffset: &off}

		tenant := claimedTenant(x.Msg.Value)
		if tenant == "" {
			b, err := json.Marshal(map[string]any{
				"contract_version": 1, "dead_letter_id": id, "quarantined_at": now.UTC().Format(time.RFC3339Nano),
				"stage": quarantineProducer, "reason_code": reason, "diagnostic": diag, "provenance": prov,
			})
			if err != nil {
				return nil, err
			}
			recs = append(recs, &kgo.Record{Topic: dlqTopic, Key: []byte("dlq\x1f" + id), Value: b})
			continue
		}
		orig := ec.NewQuarantineOriginalV1(x.Msg.Value)
		rec := ec.QuarantineRecordV1{
			ContractVersion: ec.QuarantineContractVersionV1, DeviationID: id, TenantID: tenant,
			QuarantinedAt: now.UTC(), Stage: quarantineProducer, ReasonCode: reason, Diagnostic: diag,
			Provenance: prov, Original: &orig,
		}
		if err := rec.ValidateAt(now); err != nil {
			return nil, fmt.Errorf("quarantine record invalid: %w", err)
		}
		b, err := json.Marshal(rec)
		if err != nil {
			return nil, err
		}
		recs = append(recs, &kgo.Record{Topic: quarTopic, Key: []byte(tenant + "\x1f" + id), Value: b})
	}
	return recs, nil
}

// claimedTenant returns the lower-cased tenant_id of a JSON object payload, or
// "" when absent or not a UUID.
func claimedTenant(v []byte) string {
	var t struct {
		TenantID string `json:"tenant_id"`
	}
	if json.Unmarshal(v, &t) != nil || !ec.IsUUID(t.TenantID) {
		return ""
	}
	return strings.ToLower(t.TenantID)
}

func reasonCode(c string) string {
	c = strings.ToUpper(c)
	c = strings.Map(func(r rune) rune {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, c)
	if len(c) > ec.MaxQuarantineReasonCodeBytes {
		c = c[:ec.MaxQuarantineReasonCodeBytes]
	}
	if !reasonCodeRe.MatchString(c) {
		return "ERR_UNKNOWN"
	}
	return c
}

func diagnostic(s string) string {
	s = strings.Map(func(r rune) rune {
		if r == utf8.RuneError || !unicode.IsPrint(r) {
			return ' '
		}
		return r
	}, s)
	if len(s) > ec.MaxQuarantineDiagnosticBytes {
		s = s[:ec.MaxQuarantineDiagnosticBytes]
		for !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
	}
	return s
}
