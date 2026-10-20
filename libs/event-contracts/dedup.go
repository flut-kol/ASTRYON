package eventcontracts

import "strconv"

// DedupKey builds the dedup key (tenant_id, source_id, event_id, pipeline_version).
// IDs are canonicalized so case variants collide. Returns "" if any ID is invalid
// or pipelineVersion < 1.
func DedupKey(tenantID, sourceID, eventID string, pipelineVersion int) string {
	if !IsUUID(tenantID) || !IsUUID(sourceID) || pipelineVersion < 1 {
		return ""
	}
	ev, ok := CanonicalID(eventID)
	if !ok {
		return ""
	}
	t, _ := CanonicalID(tenantID)
	s, _ := CanonicalID(sourceID)
	return t + "/" + s + "/" + ev + "/v" + strconv.Itoa(pipelineVersion)
}

// DedupKey returns the dedup key for the envelope (processing_version as pipeline version).
func (e *Envelope) DedupKey() string {
	return DedupKey(e.TenantID, e.SourceID, e.EventID, e.ProcessingVersion)
}
