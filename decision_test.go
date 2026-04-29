package junyul

import (
	"testing"
)

func TestRecordAutomatedDecision_Defaults(t *testing.T) {
	c, _ := New("JUN_test_xxx", WithEnvironment("test"))
	defer c.Close()

	id := c.RecordAutomatedDecision(DecisionInput{
		AssetID:    "credit_score_v2",
		DecisionID: "dec_abc",
		Outcome:    "approved",
		UserIDHash: "sha256:user",
	})
	if id == "" {
		t.Fatal("expected event id")
	}
	evts := c.CapturedEvents()
	if len(evts) != 1 {
		t.Fatalf("got %d events", len(evts))
	}
	ev := evts[0]
	if ev.EventType != "decision.automated_decision_made" {
		t.Fatalf("wrong type: %s", ev.EventType)
	}
	legal := ev.Metadata["legal_bases"].([]string)
	if legal[0] != "pipa_37_2_kr" {
		t.Fatalf("default legal_bases missing: %v", legal)
	}
}

func TestRecordExplanationProvided(t *testing.T) {
	c, _ := New("JUN_test_xxx", WithEnvironment("test"))
	defer c.Close()

	c.RecordExplanationProvided("a", "dec_1", "email", "sha256:user")
	evts := c.CapturedEvents()
	if evts[0].EventType != "decision.explanation_provided" {
		t.Fatalf("wrong type")
	}
}

func TestRecordHumanReviewLifecycle(t *testing.T) {
	c, _ := New("JUN_test_xxx", WithEnvironment("test"))
	defer c.Close()

	trig := c.RecordHumanReviewTriggered("a", "dec_1", "bias_concern")
	comp := c.RecordHumanReviewCompleted("a", "dec_1", "reviewer_u1", "upheld")
	if trig == "" || comp == "" {
		t.Fatal("missing ids")
	}
	evts := c.CapturedEvents()
	if len(evts) != 2 {
		t.Fatalf("expected 2, got %d", len(evts))
	}
	if evts[0].EventType != "decision.human_review_triggered" {
		t.Fatal("first event wrong type")
	}
	if evts[1].EventType != "decision.human_review_completed" {
		t.Fatal("second event wrong type")
	}
}

func TestRecordDecisionReversed(t *testing.T) {
	c, _ := New("JUN_test_xxx", WithEnvironment("test"))
	defer c.Close()

	c.RecordDecisionReversed("a", "dec_original", "denied_then_approved", "user_refusal")
	evts := c.CapturedEvents()
	if evts[0].EventType != "decision.decision_reversed" {
		t.Fatal("wrong type")
	}
	if evts[0].Metadata["triggered_by"] != "user_refusal" {
		t.Fatal("triggered_by not set")
	}
}

func TestRecordObjectionV2(t *testing.T) {
	c, _ := New("JUN_test_xxx", WithEnvironment("test"))
	defer c.Close()

	c.RecordObjectionV2(ObjectionInput{
		AssetID:       "a",
		DecisionID:    "dec_1",
		ObjectionType: "automated_decision_refusal",
		ReasonSummary: "user refused automated scoring",
		UserIDHash:    "sha256:u",
	})
	evts := c.CapturedEvents()
	if evts[0].EventType != "objection.received" {
		t.Fatal("wrong type")
	}
}

func TestRecordObjectionResolved(t *testing.T) {
	c, _ := New("JUN_test_xxx", WithEnvironment("test"))
	defer c.Close()

	c.RecordObjectionResolved("a", "obj_1", "upheld")
	evts := c.CapturedEvents()
	if evts[0].EventType != "objection.resolved" {
		t.Fatal("wrong type")
	}
	if evts[0].Metadata["resolution"] != "upheld" {
		t.Fatal("resolution not recorded")
	}
}
