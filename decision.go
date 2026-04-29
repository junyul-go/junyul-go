package junyul

import (
	"time"

	"github.com/oklog/ulid/v2"
)

// DecisionInput captures the minimum data needed to record an automated
// decision event, as required by PIPA §37조의2 ①항 and 신용정보법 §36조의2.
type DecisionInput struct {
	AssetID      string
	DecisionID   string
	Outcome      string // e.g. "approved", "denied", "flagged"
	UserIDHash   string // precomputed SHA-256 of user id (SDK never sees raw id)
	Explanation  string // machine-readable summary (no raw PII)
	LegalBases   []string
	Features     map[string]interface{} // feature names + categorical values (no raw)
}

// RecordAutomatedDecision logs an `decision.automated_decision_made` event.
//
// This is the anchor event that downstream tools (explanation, objection,
// reversal) reference via `decision_id`. The SDK never transmits raw input;
// only field names and categorical values.
func (c *Client) RecordAutomatedDecision(d DecisionInput) string {
	if len(d.LegalBases) == 0 {
		d.LegalBases = []string{"pipa_37_2_kr", "ai_basic_law_kr"}
	}
	evt := InferenceEvent{
		EventID:   "evt_" + ulid.Make().String(),
		Timestamp: time.Now().UTC(),
		AssetID:   d.AssetID,
		EventType: "decision.automated_decision_made",
		Metadata: map[string]interface{}{
			"decision_id":  d.DecisionID,
			"outcome":      d.Outcome,
			"user_id_hash": d.UserIDHash,
			"explanation":  d.Explanation,
			"legal_bases":  d.LegalBases,
			"features":     d.Features,
		},
		SDKVersion: SDKVersion,
	}
	c.Enqueue(evt)
	return evt.EventID
}

// RecordExplanationProvided logs that a §37조의2 ②항 / §36조의2 설명요구권
// response was delivered to the subject. `deliveryMethod` is one of
// "in_app", "email", "mail", "api".
func (c *Client) RecordExplanationProvided(
	assetID, decisionID, deliveryMethod, recipientHash string,
) string {
	evt := InferenceEvent{
		EventID:   "evt_" + ulid.Make().String(),
		Timestamp: time.Now().UTC(),
		AssetID:   assetID,
		EventType: "decision.explanation_provided",
		Metadata: map[string]interface{}{
			"decision_id":     decisionID,
			"delivery_method": deliveryMethod,
			"recipient_hash":  recipientHash,
			"legal_bases":     []string{"pipa_37_2_kr", "credit_info_36_2_kr"},
		},
		SDKVersion: SDKVersion,
	}
	c.Enqueue(evt)
	return evt.EventID
}

// RecordHumanReviewTriggered logs that an automated decision was escalated
// to human review (following an objection or risk threshold). Pair with
// RecordHumanReviewCompleted once the reviewer acts.
func (c *Client) RecordHumanReviewTriggered(
	assetID, decisionID, reason string,
) string {
	evt := InferenceEvent{
		EventID:   "evt_" + ulid.Make().String(),
		Timestamp: time.Now().UTC(),
		AssetID:   assetID,
		EventType: "decision.human_review_triggered",
		Metadata: map[string]interface{}{
			"decision_id": decisionID,
			"reason":      reason,
		},
		SDKVersion: SDKVersion,
	}
	c.Enqueue(evt)
	return evt.EventID
}

// RecordHumanReviewCompleted logs the outcome of a reviewer's action.
func (c *Client) RecordHumanReviewCompleted(
	assetID, decisionID, reviewerID, finalOutcome string,
) string {
	evt := InferenceEvent{
		EventID:   "evt_" + ulid.Make().String(),
		Timestamp: time.Now().UTC(),
		AssetID:   assetID,
		EventType: "decision.human_review_completed",
		Metadata: map[string]interface{}{
			"decision_id":   decisionID,
			"reviewer_id":   reviewerID,
			"final_outcome": finalOutcome,
		},
		SDKVersion: SDKVersion,
	}
	c.Enqueue(evt)
	return evt.EventID
}

// RecordDecisionReversed logs that a prior automated decision was overturned
// (PIPA §37조의2 ①항 refusal honored, or reviewer correction).
func (c *Client) RecordDecisionReversed(
	assetID, originalDecisionID, newOutcome, triggeredBy string,
) string {
	evt := InferenceEvent{
		EventID:   "evt_" + ulid.Make().String(),
		Timestamp: time.Now().UTC(),
		AssetID:   assetID,
		EventType: "decision.decision_reversed",
		Metadata: map[string]interface{}{
			"original_decision_id": originalDecisionID,
			"new_outcome":          newOutcome,
			"triggered_by":         triggeredBy, // "user_refusal" | "human_review" | "legal_hold"
		},
		SDKVersion: SDKVersion,
	}
	c.Enqueue(evt)
	return evt.EventID
}

// RecordObjectionV2 records a §37조의2 ②항 objection with richer metadata
// than the legacy RecordObjection helper. Prefer this for new integrations.
type ObjectionInput struct {
	AssetID       string
	DecisionID    string
	ObjectionType string // "automated_decision_refusal" | "explanation_request" | "appeal"
	ReasonSummary string // no raw PII
	UserIDHash    string
	LegalBases    []string
}

func (c *Client) RecordObjectionV2(o ObjectionInput) string {
	if len(o.LegalBases) == 0 {
		o.LegalBases = []string{"pipa_37_2_kr"}
	}
	evt := InferenceEvent{
		EventID:   "evt_" + ulid.Make().String(),
		Timestamp: time.Now().UTC(),
		AssetID:   o.AssetID,
		EventType: "objection.received",
		Metadata: map[string]interface{}{
			"decision_id":    o.DecisionID,
			"objection_type": o.ObjectionType,
			"reason_summary": o.ReasonSummary,
			"user_id_hash":   o.UserIDHash,
			"legal_bases":    o.LegalBases,
		},
		SDKVersion: SDKVersion,
	}
	c.Enqueue(evt)
	return evt.EventID
}

// RecordObjectionResolved closes the objection lifecycle.
func (c *Client) RecordObjectionResolved(
	assetID, objectionID, resolution string,
) string {
	evt := InferenceEvent{
		EventID:   "evt_" + ulid.Make().String(),
		Timestamp: time.Now().UTC(),
		AssetID:   assetID,
		EventType: "objection.resolved",
		Metadata: map[string]interface{}{
			"objection_id": objectionID,
			"resolution":   resolution, // "upheld" | "partial" | "denied"
		},
		SDKVersion: SDKVersion,
	}
	c.Enqueue(evt)
	return evt.EventID
}
