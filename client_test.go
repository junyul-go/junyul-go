package junyul

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNew_InvalidPrefix(t *testing.T) {
	if _, err := New("invalid"); err == nil {
		t.Fatal("expected error on invalid prefix")
	}
}

func TestTrack_TestMode_RecordsEvent(t *testing.T) {
	c, err := New("JUN_test_xxx", WithEnvironment("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	out, err := Track(context.Background(), c, "asset_1", func() (string, error) {
		return "hi", nil
	})
	if err != nil || out != "hi" {
		t.Fatalf("unexpected result out=%q err=%v", out, err)
	}

	evts := c.CapturedEvents()
	if len(evts) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evts))
	}
	if evts[0].AssetID != "asset_1" {
		t.Fatalf("unexpected asset id: %q", evts[0].AssetID)
	}
	if evts[0].OutputHash == "" {
		t.Fatal("expected output hash")
	}
	if evts[0].SDKLanguage != "go" || evts[0].Environment != "test" || evts[0].Region != "kr" {
		t.Fatalf("expected SDK envelope fields, got %#v", evts[0])
	}
}

func TestTrack_Error_RecordsFailureEvent(t *testing.T) {
	c, err := New("JUN_test_xxx", WithEnvironment("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	sentinel := errors.New("denied for user 123")
	_, err = Track(context.Background(), c, "asset_1", func() (string, error) {
		return "", sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}

	evts := c.CapturedEvents()
	if len(evts) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evts))
	}
	if evts[0].OutputHash != "" {
		t.Fatal("expected no output hash for failed call")
	}
	if evts[0].Metadata["error"] != true {
		t.Fatalf("expected error=true metadata: %#v", evts[0].Metadata)
	}
	if _, ok := evts[0].Metadata["error_message"]; ok {
		t.Fatal("raw error message must not be emitted")
	}
	hash, _ := evts[0].Metadata["error_message_hash"].(string)
	if !strings.HasPrefix(hash, "sha256:") {
		t.Fatalf("expected hashed error message, got %q", hash)
	}
}

func TestTrack_Panic_RecordsFailureEvent(t *testing.T) {
	c, err := New("JUN_test_xxx", WithEnvironment("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected panic")
		}
		evts := c.CapturedEvents()
		if len(evts) != 1 {
			t.Fatalf("expected 1 event, got %d", len(evts))
		}
		if evts[0].Metadata["panic"] != true {
			t.Fatalf("expected panic=true metadata: %#v", evts[0].Metadata)
		}
		hash, _ := evts[0].Metadata["panic_message_hash"].(string)
		if !strings.HasPrefix(hash, "sha256:") {
			t.Fatalf("expected hashed panic message, got %q", hash)
		}
	}()

	_, _ = Track(context.Background(), c, "asset_1", func() (string, error) {
		panic("sensitive panic")
	})
}

func TestRecordHeartbeat_RecordsHealth(t *testing.T) {
	c, err := New("JUN_test_xxx", WithEnvironment("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	id := c.RecordHeartbeat("asset_1")

	evts := c.CapturedEvents()
	if !strings.HasPrefix(id, "evt_") {
		t.Fatalf("expected event id, got %q", id)
	}
	if len(evts) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evts))
	}
	if evts[0].EventType != "sdk.heartbeat" {
		t.Fatalf("wrong type: %s", evts[0].EventType)
	}
	if evts[0].Metadata["circuit_state"] != "closed" {
		t.Fatalf("expected closed circuit metadata: %#v", evts[0].Metadata)
	}
	if evts[0].Metadata["sdk_language"] != "go" {
		t.Fatalf("expected go sdk language: %#v", evts[0].Metadata)
	}
	if evts[0].SDKLanguage != "go" || evts[0].Environment != "test" || evts[0].Region != "kr" {
		t.Fatalf("expected SDK envelope fields, got %#v", evts[0])
	}
}

func TestClient_Flush_ParksFailedEventsInOutboxAndOpensCircuit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c, err := New(
		"JUN_test_xxx",
		WithEnvironment("production"),
		WithEndpoint(srv.URL),
		WithHTTPClient(srv.Client()),
		WithOutbox(filepath.Join(t.TempDir(), "outbox.ndjson")),
		WithCircuitBreaker(1, time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.Enqueue(InferenceEvent{
		EventID:    "evt_test",
		Timestamp:  time.Now().UTC(),
		AssetID:    "asset_1",
		EventType:  "ai_inference",
		SDKVersion: SDKVersion,
	})

	if err := c.Flush(context.Background()); err == nil {
		t.Fatal("expected flush error")
	}
	n, err := c.outbox.Count()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected one event in outbox, got %d", n)
	}
	if c.circuit.State() != CircuitOpen {
		t.Fatalf("expected open circuit, got %s", c.circuit.State())
	}
}
