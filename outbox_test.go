package junyul

import (
	"path/filepath"
	"testing"
	"time"
)

func TestOutbox_PutAndDrainRoundtrip(t *testing.T) {
	dir := t.TempDir()
	ob, err := NewOutbox(filepath.Join(dir, "outbox.ndjson"), 1000, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer ob.Close()

	evt := InferenceEvent{
		EventID:    "evt_1",
		Timestamp:  time.Now().UTC(),
		AssetID:    "a",
		EventType:  "ai_inference",
		SDKVersion: SDKVersion,
	}
	if err := ob.Put(evt); err != nil {
		t.Fatal(err)
	}

	count, _ := ob.Count()
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}

	drained, err := ob.Drain(100)
	if err != nil {
		t.Fatal(err)
	}
	if len(drained) != 1 {
		t.Fatalf("expected drained=1, got %d", len(drained))
	}
	if drained[0].EventID != "evt_1" {
		t.Fatalf("wrong event: %v", drained[0].EventID)
	}

	count2, _ := ob.Count()
	if count2 != 0 {
		t.Fatalf("expected 0 after drain, got %d", count2)
	}
}

func TestOutbox_PutManyBatch(t *testing.T) {
	dir := t.TempDir()
	ob, _ := NewOutbox(filepath.Join(dir, "out.ndjson"), 100, 7)
	defer ob.Close()

	events := []InferenceEvent{
		{EventID: "e1", Timestamp: time.Now().UTC(), AssetID: "a", EventType: "x", SDKVersion: SDKVersion},
		{EventID: "e2", Timestamp: time.Now().UTC(), AssetID: "a", EventType: "x", SDKVersion: SDKVersion},
		{EventID: "e3", Timestamp: time.Now().UTC(), AssetID: "a", EventType: "x", SDKVersion: SDKVersion},
	}
	if err := ob.PutMany(events); err != nil {
		t.Fatal(err)
	}
	count, _ := ob.Count()
	if count != 3 {
		t.Fatalf("expected 3, got %d", count)
	}
}

func TestOutbox_DrainRespectsMax(t *testing.T) {
	dir := t.TempDir()
	ob, _ := NewOutbox(filepath.Join(dir, "out.ndjson"), 100, 7)
	defer ob.Close()

	for i := 0; i < 5; i++ {
		_ = ob.Put(InferenceEvent{
			EventID: "e_" + string(rune('0'+i)), Timestamp: time.Now().UTC(),
			AssetID: "a", EventType: "x", SDKVersion: SDKVersion,
		})
	}

	first, _ := ob.Drain(2)
	if len(first) != 2 {
		t.Fatalf("expected 2, got %d", len(first))
	}
	remaining, _ := ob.Count()
	if remaining != 3 {
		t.Fatalf("expected 3 remaining, got %d", remaining)
	}
}

func TestOutbox_DrainSkipsExpiredByTTL(t *testing.T) {
	dir := t.TempDir()
	ob, _ := NewOutbox(filepath.Join(dir, "out.ndjson"), 100, 1)
	defer ob.Close()

	// Past timestamp — older than 1 day
	old := InferenceEvent{
		EventID: "old", Timestamp: time.Now().Add(-48 * time.Hour).UTC(),
		AssetID: "a", EventType: "x", SDKVersion: SDKVersion,
	}
	fresh := InferenceEvent{
		EventID: "fresh", Timestamp: time.Now().UTC(),
		AssetID: "a", EventType: "x", SDKVersion: SDKVersion,
	}
	_ = ob.PutMany([]InferenceEvent{old, fresh})

	drained, _ := ob.Drain(10)
	if len(drained) != 1 {
		t.Fatalf("expected 1 (TTL filter), got %d", len(drained))
	}
	if drained[0].EventID != "fresh" {
		t.Fatalf("wrong event survived: %s", drained[0].EventID)
	}
}

func TestOutbox_EmptyPath(t *testing.T) {
	if _, err := NewOutbox("", 0, 0); err == nil {
		t.Fatal("expected error on empty path")
	}
}

func TestOutbox_DrainEmptyFileReturnsNil(t *testing.T) {
	dir := t.TempDir()
	ob, _ := NewOutbox(filepath.Join(dir, "out.ndjson"), 100, 7)
	defer ob.Close()

	out, err := ob.Drain(10)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil {
		t.Fatalf("expected nil, got %v", out)
	}
}
