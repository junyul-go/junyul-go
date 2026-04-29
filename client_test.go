package junyul

import (
	"context"
	"testing"
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
}
