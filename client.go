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

const SDKVersion = "1.2.1"

var errCircuitOpen = errors.New("junyul: transport circuit is open")

// Client is the main entry point. Safe for concurrent use.
type Client struct {
	apiKey       string
	endpoint     string
	environment  string
	region       string
	userAgent    string
	http         *http.Client
	maxBatchSize int
	maxQueueSize int

	outboxEnabled           bool
	outboxPath              string
	outboxMaxRows           int
	outboxTTLDays           int
	circuitFailureThreshold int
	circuitCooldown         time.Duration
	outbox                  *Outbox
	circuit                 *CircuitBreaker

	mu       sync.Mutex
	buf      []InferenceEvent
	stopCh   chan struct{}
	done     chan struct{}
	captured []InferenceEvent
}

// Config contains client options.
type Config struct {
	APIKey      string
	Endpoint    string
	Environment string
	Region      string
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
		apiKey:                  apiKey,
		endpoint:                "https://api.junyul.com",
		environment:             "production",
		region:                  "kr",
		userAgent:               "junyul-go/" + SDKVersion,
		http:                    &http.Client{Timeout: 10 * time.Second},
		maxBatchSize:            100,
		maxQueueSize:            10_000,
		outboxEnabled:           true,
		outboxPath:              ".junyul/outbox.ndjson",
		outboxMaxRows:           100_000,
		outboxTTLDays:           7,
		circuitFailureThreshold: 5,
		circuitCooldown:         30 * time.Second,
		stopCh:                  make(chan struct{}),
		done:                    make(chan struct{}),
	}
	for _, o := range opts {
		o(c)
	}
	c.circuit = NewCircuitBreaker(c.circuitFailureThreshold, c.circuitCooldown)
	if c.outboxEnabled && c.environment != "test" {
		ob, err := NewOutbox(c.outboxPath, c.outboxMaxRows, c.outboxTTLDays)
		if err != nil {
			return nil, err
		}
		c.outbox = ob
	}
	go c.runFlusher()
	return c, nil
}

// Option configures the client.
type Option func(*Client)

func WithEndpoint(url string) Option    { return func(c *Client) { c.endpoint = url } }
func WithEnvironment(env string) Option { return func(c *Client) { c.environment = env } }
func WithRegion(region string) Option   { return func(c *Client) { c.region = region } }
func WithOutbox(path string) Option {
	return func(c *Client) {
		c.outboxEnabled = true
		c.outboxPath = path
	}
}
func WithoutOutbox() Option { return func(c *Client) { c.outboxEnabled = false } }
func WithCircuitBreaker(failureThreshold int, cooldown time.Duration) Option {
	return func(c *Client) {
		c.circuitFailureThreshold = failureThreshold
		c.circuitCooldown = cooldown
	}
}
func WithQueueLimits(maxQueueSize, maxBatchSize int) Option {
	return func(c *Client) {
		if maxQueueSize > 0 {
			c.maxQueueSize = maxQueueSize
		}
		if maxBatchSize > 0 {
			c.maxBatchSize = maxBatchSize
		}
	}
}
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
	err := c.Flush(context.Background())
	if c.outbox != nil {
		if outboxErr := c.outbox.Close(); err == nil {
			err = outboxErr
		}
	}
	return err
}

// Flush forces immediate delivery of the current buffer.
func (c *Client) Flush(ctx context.Context) error {
	c.mu.Lock()
	buf := c.buf
	c.buf = nil
	c.mu.Unlock()
	if len(buf) == 0 {
		return c.drainOutbox(ctx)
	}
	if err := c.sendEvents(ctx, buf); err != nil {
		if c.outbox != nil {
			if outboxErr := c.outbox.PutMany(buf); outboxErr != nil {
				c.requeueFront(buf)
				return fmt.Errorf("%w; outbox: %v", err, outboxErr)
			}
			return err
		}
		c.requeueFront(buf)
		return err
	}
	return c.drainOutbox(ctx)
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
	c.enrichEvent(&event)
	if c.environment == "test" {
		c.mu.Lock()
		c.captured = append(c.captured, event)
		c.mu.Unlock()
		return
	}
	c.mu.Lock()
	c.buf = append(c.buf, event)
	if len(c.buf) > c.maxQueueSize {
		c.buf = c.buf[len(c.buf)-c.maxQueueSize:]
	}
	shouldFlush := len(c.buf) >= c.maxBatchSize
	c.mu.Unlock()
	if shouldFlush {
		go func() { _ = c.Flush(context.Background()) }()
	}
}

func (c *Client) enrichEvent(event *InferenceEvent) {
	if event.SDKVersion == "" {
		event.SDKVersion = SDKVersion
	}
	if event.SDKLanguage == "" {
		event.SDKLanguage = "go"
	}
	if event.Environment == "" {
		event.Environment = c.environment
	}
	if event.Region == "" {
		event.Region = c.region
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
	for i := range events {
		c.enrichEvent(&events[i])
	}
	if c.circuit != nil && !c.circuit.Allow() {
		return errCircuitOpen
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
		if c.circuit != nil {
			c.circuit.RecordFailure()
		}
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		err := fmt.Errorf("junyul: ingest returned %d", resp.StatusCode)
		if c.circuit != nil {
			c.circuit.RecordFailure()
		}
		return err
	}
	if c.circuit != nil {
		c.circuit.RecordSuccess()
	}
	return nil
}

func (c *Client) requeueFront(events []InferenceEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf = append(append([]InferenceEvent{}, events...), c.buf...)
	if len(c.buf) > c.maxQueueSize {
		c.buf = c.buf[:c.maxQueueSize]
	}
}

func (c *Client) drainOutbox(ctx context.Context) error {
	if c.outbox == nil || c.environment == "test" {
		return nil
	}
	if c.circuit != nil && !c.circuit.Allow() {
		return nil
	}
	pending, err := c.outbox.Drain(c.maxBatchSize)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}
	if err := c.sendEvents(ctx, pending); err != nil {
		_ = c.outbox.PutMany(pending)
		return err
	}
	return nil
}

// Health exposes transport/reliability state for SDK telemetry dashboards.
func (c *Client) Health() map[string]interface{} {
	c.mu.Lock()
	queueDepth := len(c.buf)
	c.mu.Unlock()
	outboxDepth := 0
	if c.outbox != nil {
		if n, err := c.outbox.Count(); err == nil {
			outboxDepth = n
		}
	}
	circuitState := "unknown"
	if c.circuit != nil {
		circuitState = c.circuit.State().String()
	}
	return map[string]interface{}{
		"version":       SDKVersion,
		"queue_depth":   queueDepth,
		"outbox_depth":  outboxDepth,
		"circuit_state": circuitState,
	}
}

// InferenceEvent matches the ClickHouse event envelope.
type InferenceEvent struct {
	EventID     string                 `json:"event_id"`
	Timestamp   time.Time              `json:"timestamp"`
	AssetID     string                 `json:"asset_id"`
	EventType   string                 `json:"event_type"`
	InputHash   string                 `json:"input_hash,omitempty"`
	OutputHash  string                 `json:"output_hash,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	SDKVersion  string                 `json:"sdk_version"`
	SDKLanguage string                 `json:"sdk_language,omitempty"`
	Environment string                 `json:"environment,omitempty"`
	Region      string                 `json:"region,omitempty"`
	Host        map[string]string      `json:"host,omitempty"`
}

// Track runs fn and records an ai_inference event. Panics and errors
// propagate to the caller; event is still recorded with error=true metadata.
func Track[T any](ctx context.Context, c *Client, assetID string, fn func() (T, error)) (result T, err error) {
	start := time.Now()
	defer func() {
		if recovered := recover(); recovered != nil {
			duration := time.Since(start)
			meta := map[string]interface{}{"duration_ms": duration.Milliseconds()}
			for k, v := range panicMetadata(recovered) {
				meta[k] = v
			}
			c.enqueueTrackEvent(assetID, "", meta)
			panic(recovered)
		}
	}()

	result, err = fn()
	duration := time.Since(start)

	meta := map[string]interface{}{"duration_ms": duration.Milliseconds()}
	if err != nil {
		for k, v := range errorMetadata(err) {
			meta[k] = v
		}
	}
	outputHash := ""
	if err == nil {
		h, hashErr := hashValue(result)
		if hashErr == nil {
			outputHash = h
		}
	}

	c.enqueueTrackEvent(assetID, outputHash, meta)
	return result, err
}

func (c *Client) enqueueTrackEvent(assetID, outputHash string, metadata map[string]interface{}) {
	evt := InferenceEvent{
		EventID:    "evt_" + ulid.Make().String(),
		Timestamp:  time.Now().UTC(),
		AssetID:    assetID,
		EventType:  "ai_inference",
		OutputHash: outputHash,
		Metadata:   metadata,
		SDKVersion: SDKVersion,
		Host:       map[string]string{"runtime": "go", "os": ""},
	}
	c.Enqueue(evt)
}

func hashValue(v interface{}) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func hashString(v string) string {
	sum := sha256.Sum256([]byte(v))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func errorMetadata(err error) map[string]interface{} {
	meta := map[string]interface{}{
		"error":      true,
		"error_type": fmt.Sprintf("%T", err),
	}
	if err != nil && err.Error() != "" {
		meta["error_message_hash"] = hashString(err.Error())
	}
	return meta
}

func panicMetadata(recovered interface{}) map[string]interface{} {
	message := fmt.Sprint(recovered)
	meta := map[string]interface{}{
		"error":      true,
		"panic":      true,
		"panic_type": fmt.Sprintf("%T", recovered),
	}
	if message != "" {
		meta["panic_message_hash"] = hashString(message)
	}
	return meta
}

// RecordHeartbeat records SDK transport state for operational telemetry.
func (c *Client) RecordHeartbeat(assetID string) string {
	health := c.Health()
	evt := InferenceEvent{
		EventID:   "evt_" + ulid.Make().String(),
		Timestamp: time.Now().UTC(),
		AssetID:   assetID,
		EventType: "sdk.heartbeat",
		Metadata: map[string]interface{}{
			"queue_depth":   health["queue_depth"],
			"outbox_depth":  health["outbox_depth"],
			"circuit_state": health["circuit_state"],
			"sdk_version":   SDKVersion,
			"sdk_language":  "go",
			"environment":   c.environment,
			"region":        c.region,
		},
		SDKVersion: SDKVersion,
		Host:       map[string]string{"runtime": "go", "os": ""},
	}
	c.Enqueue(evt)
	return evt.EventID
}

// RecordObjection records a user objection (법 제33조 제3호).
func (c *Client) RecordObjection(assetID, decisionID, reason, userIDHash string) string {
	evt := InferenceEvent{
		EventID:   "obj_" + ulid.Make().String(),
		Timestamp: time.Now().UTC(),
		AssetID:   assetID,
		EventType: "objection",
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
		EventID:   "rev_" + ulid.Make().String(),
		Timestamp: time.Now().UTC(),
		AssetID:   assetID,
		EventType: "human_review",
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
