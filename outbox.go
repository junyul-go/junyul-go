package junyul

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Outbox is a file-backed fallback queue for events that failed to transmit.
// Unlike the Python/TS SDKs (which use SQLite), Go's stdlib keeps this light:
// one newline-delimited JSON file per buffer generation, rotated on drain.
//
// The Go runtime has no atexit hook, so callers MUST call (*Client).Close()
// before process exit to guarantee durability.
type Outbox struct {
	path      string
	maxRows   int
	ttlDays   int
	mu        sync.Mutex
	fileLock  sync.Mutex
}

// NewOutbox opens (creates if missing) a newline-JSON outbox file.
func NewOutbox(path string, maxRows, ttlDays int) (*Outbox, error) {
	if path == "" {
		return nil, fmt.Errorf("outbox: path required")
	}
	if maxRows <= 0 {
		maxRows = 100_000
	}
	if ttlDays <= 0 {
		ttlDays = 7
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("outbox: mkdir: %w", err)
	}
	// Touch the file so readers can always open it.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("outbox: create: %w", err)
	}
	_ = f.Close()
	return &Outbox{path: path, maxRows: maxRows, ttlDays: ttlDays}, nil
}

// Put appends one event.
func (o *Outbox) Put(event InferenceEvent) error {
	return o.PutMany([]InferenceEvent{event})
}

// PutMany appends a batch atomically (single write syscall).
func (o *Outbox) PutMany(events []InferenceEvent) error {
	if len(events) == 0 {
		return nil
	}
	o.fileLock.Lock()
	defer o.fileLock.Unlock()

	f, err := os.OpenFile(o.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, ev := range events {
		raw, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		if _, err := f.Write(append(raw, '\n')); err != nil {
			return err
		}
	}
	return nil
}

// Drain returns up to max events and removes them from the outbox.
// Caller is expected to only call Drain after confirming transport is healthy.
func (o *Outbox) Drain(max int) ([]InferenceEvent, error) {
	if max <= 0 {
		max = 100
	}
	o.fileLock.Lock()
	defer o.fileLock.Unlock()

	raw, err := os.ReadFile(o.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}

	// Parse all lines, skipping TTL-expired and malformed entries.
	var all []InferenceEvent
	cutoff := time.Now().Add(-time.Duration(o.ttlDays) * 24 * time.Hour)
	for _, line := range splitLines(raw) {
		if len(line) == 0 {
			continue
		}
		var ev InferenceEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			continue
		}
		if ev.Timestamp.Before(cutoff) {
			continue
		}
		all = append(all, ev)
	}

	// Take up to `max` from the head; write the remainder back.
	n := min(len(all), max)
	head := all[:n]
	rest := all[n:]

	// Rewrite the file with `rest`.
	tmp := o.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	enc := json.NewEncoder(f)
	for _, ev := range rest {
		if err := enc.Encode(ev); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return nil, err
		}
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, o.path); err != nil {
		return nil, err
	}
	return head, nil
}

// Count returns the number of entries currently in the outbox.
func (o *Outbox) Count() (int, error) {
	o.fileLock.Lock()
	defer o.fileLock.Unlock()

	raw, err := os.ReadFile(o.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	n := 0
	for _, line := range splitLines(raw) {
		if len(line) > 0 {
			n++
		}
	}
	return n, nil
}

// Close is a no-op placeholder symmetric with other SDKs — the Go outbox
// uses per-call file handles, no persistent connections to release.
func (o *Outbox) Close() error { return nil }

func splitLines(b []byte) [][]byte {
	var out [][]byte
	start := 0
	for i := 0; i < len(b); i++ {
		if b[i] == '\n' {
			out = append(out, b[start:i])
			start = i + 1
		}
	}
	if start < len(b) {
		out = append(out, b[start:])
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
