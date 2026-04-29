package junyul

import (
	"time"

	"github.com/oklog/ulid/v2"
)

// RecordTransparencyNoticeShown records that a disclosure banner was rendered
// to the end-user (AI 기본법 §31, EU AI Act Art. 50).
//
// Legal basis defaults to ["ai_basic_law_kr", "eu_ai_act_art_50"]; callers can
// override via legalBases if a different combination applies.
func (c *Client) RecordTransparencyNoticeShown(
	assetID, locale, channel string,
	legalBases []string,
) string {
	if len(legalBases) == 0 {
		legalBases = []string{"ai_basic_law_kr", "eu_ai_act_art_50"}
	}
	evt := InferenceEvent{
		EventID:   "evt_" + ulid.Make().String(),
		Timestamp: time.Now().UTC(),
		AssetID:   assetID,
		EventType: "transparency.notice_shown",
		Metadata: map[string]interface{}{
			"legal_bases":    legalBases,
			"notice_channel": channel,
			"locale":         locale,
		},
		SDKVersion: SDKVersion,
	}
	c.Enqueue(evt)
	return evt.EventID
}

// RecordTransparencyPolicyDisclosed records that the tenant's public AI-use
// policy page was linked/rendered to the user. Separate from notice_shown:
// this is the "policy exists and was exposed" event.
func (c *Client) RecordTransparencyPolicyDisclosed(
	assetID, policyURL, version string,
) string {
	evt := InferenceEvent{
		EventID:   "evt_" + ulid.Make().String(),
		Timestamp: time.Now().UTC(),
		AssetID:   assetID,
		EventType: "transparency.policy_disclosed",
		Metadata: map[string]interface{}{
			"policy_url":     policyURL,
			"policy_version": version,
		},
		SDKVersion: SDKVersion,
	}
	c.Enqueue(evt)
	return evt.EventID
}

// RecordResultLabeled records that a generative output was programmatically
// labeled as AI-generated (EU AI Act Art. 50(2) deepfake labeling).
func (c *Client) RecordResultLabeled(
	assetID, labelType, outputHash string,
) string {
	evt := InferenceEvent{
		EventID:    "evt_" + ulid.Make().String(),
		Timestamp:  time.Now().UTC(),
		AssetID:    assetID,
		EventType:  "transparency.result_labeled",
		OutputHash: outputHash,
		Metadata: map[string]interface{}{
			"label_type":  labelType,
			"legal_bases": []string{"eu_ai_act_art_50", "ai_basic_law_kr"},
		},
		SDKVersion: SDKVersion,
	}
	c.Enqueue(evt)
	return evt.EventID
}
