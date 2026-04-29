package junyul

import (
	"testing"
)

func TestRecordTransparencyNoticeShown(t *testing.T) {
	c, err := New("JUN_test_xxx", WithEnvironment("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	id := c.RecordTransparencyNoticeShown("asset_a", "ko", "http_banner", nil)
	if id == "" {
		t.Fatal("expected event id")
	}
	evts := c.CapturedEvents()
	if len(evts) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evts))
	}
	if evts[0].EventType != "transparency.notice_shown" {
		t.Fatalf("wrong event type: %s", evts[0].EventType)
	}
	legal, ok := evts[0].Metadata["legal_bases"].([]string)
	if !ok || len(legal) == 0 {
		t.Fatal("missing legal_bases")
	}
}

func TestRecordTransparencyNoticeShown_CustomLegalBases(t *testing.T) {
	c, _ := New("JUN_test_xxx", WithEnvironment("test"))
	defer c.Close()

	c.RecordTransparencyNoticeShown("a", "en", "banner", []string{"gdpr_art_22"})
	evts := c.CapturedEvents()
	legal := evts[0].Metadata["legal_bases"].([]string)
	if legal[0] != "gdpr_art_22" {
		t.Fatalf("legal_bases not overridden: %v", legal)
	}
}

func TestRecordTransparencyPolicyDisclosed(t *testing.T) {
	c, _ := New("JUN_test_xxx", WithEnvironment("test"))
	defer c.Close()

	c.RecordTransparencyPolicyDisclosed("a", "https://junyul.com/policy", "v1.2")
	evts := c.CapturedEvents()
	if evts[0].EventType != "transparency.policy_disclosed" {
		t.Fatalf("wrong event type")
	}
	if evts[0].Metadata["policy_url"] != "https://junyul.com/policy" {
		t.Fatal("policy_url not set")
	}
}

func TestRecordResultLabeled(t *testing.T) {
	c, _ := New("JUN_test_xxx", WithEnvironment("test"))
	defer c.Close()

	c.RecordResultLabeled("a", "deepfake", "sha256:abc")
	evts := c.CapturedEvents()
	if evts[0].EventType != "transparency.result_labeled" {
		t.Fatalf("wrong event type")
	}
	if evts[0].OutputHash != "sha256:abc" {
		t.Fatal("output_hash not preserved")
	}
}
