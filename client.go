// Package junyul is the Go SDK for the Junyul compliance platform.
// Three-line integration:
//
//	client, _ := junyul.New("JUN_live_xxx")
//	defer client.Close()
//	out, err := junyul.Track(ctx, client, "asset_id", func() (string, error) { return ... })
package junyul

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

const SDKVersion = "1.0.0"

// Client is the main entry point. Safe for concurrent use.
type Client struct {
	apiKey      string
	endpoint    string
	environment string
	userAgent   string
	http        *http.Client

	mu      sync.Mutex
	buf     []InferenceEvent
	stopCh  chan struct{}
	done    chan struct{}
	captured []InferenceEvent
}

// Config contains client options.
type Config struct {
	APIKey      string
	Endpoint    string
	Environment string
	HTTPClient  *http.Client
}

// New creates a new client. Pass an API key starting with JUN_live_ or JUN_test_.
func New(apiKey string, opts ...Option) (*Client, error) {
	if apiKey == "" {
		return nil, errors.New("apiKey required")
	}
	if !strings.HasPrefix(apiKey, "JUN_live_") && !strings.HasPrefix(apiKey, "JUN_test_") {
		return nil, errors.New("apiKey must start with JUN_live_ or JUN_test_")
	}
	c := &Client{
		apiKey:      apiKey,
		endpoint:    "https://api.junyul.com",
		environment: "production",
		userAgent:   "junyul-go/" + SDKVersion,
		http:        &http.Client{Timeout: 10 * time.Second},
		stopCh:      make(chan struct{}),
		done:        make(chan struct{}),
	}
	for _, o := range opts {
		o(c)
	}
	go c.runFlusher()
	return c, nil
}

// Option configures the client.
type Option func(*Client)

func WithEndpoint(url string) Option    { return func(c *Client) { c.endpoint = url } }
func WithEnvironment(env string) Option { return func(c *Client) { c.environment = env } }
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) {
		if h != nil {
			c.http = h
		}
	}
}

// Close flushes any pending events and stops background workers.
func (c *Client) Close() error {
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
	<-c.done
	return c.Flush(context.Background())
}

// Flush forces immediate delivery of the current buffer.
func (c *Client) Flush(ctx context.Context) error {
	c.mu.Lock()
	buf := c.buf
	c.buf = nil
	c.mu.Unlock()
	if len(buf) == 0 {
		return nil
	}
	return c.sendEvents(ctx, buf)
}

func (c *Client) runFlusher() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	defer close(c.done)
	for {
		select {
		case <-c.stopCh:
			return
		case <-t.C:
			_ = c.Flush(context.Background())
		}
	}
}

// Enqueue adds an event to the buffer.
func (c *Client) Enqueue(event InferenceEvent) {
	if c.environment == "test" {
		c.mu.Lock()
		c.captured = append(c.captured, event)
		c.mu.Unlock()
		return
	}
	c.mu.Lock()
	c.buf = append(c.buf, event)
	shouldFlush := len(c.buf) >= 100
	c.mu.Unlock()
	if shouldFlush {
		go func() { _ = c.Flush(context.Background()) }()
	}
}

// CapturedEvents returns test-mode captured events.
func (c *Client) CapturedEvents() []InferenceEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]InferenceEvent, len(c.captured))
	copy(out, c.captured)
	return out
}

// ResetCaptured clears the test-mode buffer.
func (c *Client) ResetCaptured() {
	c.mu.Lock()
	c.captured = nil
	c.mu.Unlock()
}

func (c *Client) sendEvents(ctx context.Context, events []InferenceEvent) error {
	if c.environment == "test" {
		return nil
	}
	body, err := json.Marshal(map[string][]InferenceEvent{"events": events})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/v1/events/batch", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("junyul: ingest returned %d", resp.StatusCode)
	}
	return nil
}

// InferenceEvent matches the ClickHouse event envelope.
type InferenceEvent struct {
	EventID    string                 `json:"event_id"`
	Timestamp  time.Time              `json:"timestamp"`
	AssetID    string                 `json:"asset_id"`
	EventType  string                 `json:"event_type"`
	InputHash  string                 `json:"input_hash,omitempty"`
	OutputHash string                 `json:"output_hash,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
	SDKVersion string                 `json:"sdk_version"`
	Host       map[string]string      `json:"host,omitempty"`
}

// Track runs ``fn`` and records an ai_inference event. Panics and errors
// propagate to the caller; event is still recorded with ``error=true`` metadata.
func Track[T any](ctx context.Context, c *Client, assetID string, fn func() (T, error)) (T, error) {
	start := time.Now()
	result, err := fn()
	duration := time.Since(start)

	meta := map[string]interface{}{"duration_ms": duration.Milliseconds()}
	if err != nil {
		meta["error"] = err.Error()
	}
	outputHash := ""
	if err == nil {
		h, hashErr := hashValue(result)
		if hashErr == nil {
			outputHash = h
		}
	}

	evt := InferenceEvent{
		EventID:    "evt_" + ulid.Make().String(),
		Timestamp:  time.Now().UTC(),
		AssetID:    assetID,
		EventType:  "ai_inference",
		OutputHash: outputHash,
		Metadata:   meta,
		SDKVersion: SDKVersion,
		Host:       map[string]string{"runtime": "go", "os": ""},
	}
	c.Enqueue(evt)
	return result, err
}

func hashValue(v interface{}) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// RecordObjection records a user objection (법 제33조 제3호).
func (c *Client) RecordObjection(assetID, decisionID, reason, userIDHash string) string {
	evt := InferenceEvent{
		EventID:    "obj_" + ulid.Make().String(),
		Timestamp:  time.Now().UTC(),
		AssetID:    assetID,
		EventType:  "objection",
		Metadata: map[string]interface{}{
			"decision_id":  decisionID,
			"reason":       reason,
			"user_id_hash": userIDHash,
		},
		SDKVersion: SDKVersion,
	}
	c.Enqueue(evt)
	return evt.EventID
}

// RecordHumanReview records a human-in-the-loop review.
func (c *Client) RecordHumanReview(assetID, decisionID, reviewerID, outcome string) string {
	evt := InferenceEvent{
		EventID:    "rev_" + ulid.Make().String(),
		Timestamp:  time.Now().UTC(),
		AssetID:    assetID,
		EventType:  "human_review",
		Metadata: map[string]interface{}{
			"decision_id": decisionID,
			"reviewer_id": reviewerID,
			"outcome":     outcome,
		},
		SDKVersion: SDKVersion,
	}
	c.Enqueue(evt)
	return evt.EventID
}

// Classify calls the /v1/classify endpoint.
func (c *Client) Classify(ctx context.Context, req ClassificationRequest) (*ClassificationResult, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/v1/classify", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", c.userAgent)
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("junyul: classify returned %d: %s", resp.StatusCode, string(raw))
	}
	var wrapped struct {
		Data ClassificationResult `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&wrapped); err != nil {
		return nil, err
	}
	return &wrapped.Data, nil
}
