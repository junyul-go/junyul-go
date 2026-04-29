package junyul

import (
	"sync"
	"time"
)

// CircuitState represents the state of the transport circuit breaker.
type CircuitState int

const (
	// CircuitClosed means requests flow normally.
	CircuitClosed CircuitState = iota
	// CircuitOpen means recent failures tripped the breaker; requests are dropped
	// or queued to the outbox until the cooldown elapses.
	CircuitOpen
	// CircuitHalfOpen means the cooldown has elapsed; the next request is a probe.
	CircuitHalfOpen
)

// String returns the human-readable state name.
func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half_open"
	}
	return "unknown"
}

// CircuitBreaker is a thread-safe transport-level circuit breaker.
// Zero value is unusable; call NewCircuitBreaker.
type CircuitBreaker struct {
	failureThreshold int
	cooldown         time.Duration

	mu             sync.Mutex
	state          CircuitState
	failures       int
	openedAt       time.Time
	lastTransition time.Time
}

// NewCircuitBreaker constructs a breaker with the given thresholds.
// Defaults used when zero values are passed: 5 failures, 30s cooldown.
func NewCircuitBreaker(failureThreshold int, cooldown time.Duration) *CircuitBreaker {
	if failureThreshold <= 0 {
		failureThreshold = 5
	}
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	return &CircuitBreaker{
		failureThreshold: failureThreshold,
		cooldown:         cooldown,
		state:            CircuitClosed,
	}
}

// Allow returns true if the caller is permitted to proceed with the request.
// Transitions OPEN → HALF_OPEN once the cooldown has elapsed.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == CircuitOpen && time.Since(cb.openedAt) >= cb.cooldown {
		cb.state = CircuitHalfOpen
		cb.lastTransition = time.Now()
	}
	return cb.state != CircuitOpen
}

// RecordSuccess resets the breaker after a passing request.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
	if cb.state != CircuitClosed {
		cb.state = CircuitClosed
		cb.lastTransition = time.Now()
	}
}

// RecordFailure increments the failure count and opens the circuit if the
// threshold is crossed, or flips back OPEN if a HALF_OPEN probe fails.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	if cb.state == CircuitHalfOpen || cb.failures >= cb.failureThreshold {
		cb.state = CircuitOpen
		cb.openedAt = time.Now()
		cb.lastTransition = cb.openedAt
	}
}

// State returns the current state; safe for telemetry export.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// Failures returns the current failure count (reset on any success).
func (cb *CircuitBreaker) Failures() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.failures
}
