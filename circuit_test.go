package junyul

import (
	"testing"
	"time"
)

func TestCircuit_OpensAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(3, 100*time.Millisecond)
	for i := 0; i < 3; i++ {
		if !cb.Allow() {
			t.Fatalf("Allow() returned false too early at i=%d", i)
		}
		cb.RecordFailure()
	}
	if cb.State() != CircuitOpen {
		t.Fatalf("expected OPEN, got %s", cb.State())
	}
	if cb.Allow() {
		t.Fatal("Allow should be false when OPEN")
	}
}

func TestCircuit_HalfOpenAfterCooldown(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Millisecond)
	cb.RecordFailure()
	if cb.State() != CircuitOpen {
		t.Fatal("should be OPEN after 1 failure")
	}
	time.Sleep(15 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("Allow should unlock after cooldown")
	}
	if cb.State() != CircuitHalfOpen {
		t.Fatalf("expected HALF_OPEN, got %s", cb.State())
	}
}

func TestCircuit_HalfOpenFailureReopens(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Millisecond)
	cb.RecordFailure()
	time.Sleep(15 * time.Millisecond)
	_ = cb.Allow() // transitions to HALF_OPEN
	cb.RecordFailure()
	if cb.State() != CircuitOpen {
		t.Fatalf("HALF_OPEN → OPEN on failure, got %s", cb.State())
	}
}

func TestCircuit_SuccessResetsToClosed(t *testing.T) {
	cb := NewCircuitBreaker(2, 10*time.Millisecond)
	cb.RecordFailure()
	cb.RecordSuccess()
	if cb.State() != CircuitClosed {
		t.Fatalf("should reset to CLOSED, got %s", cb.State())
	}
	if cb.Failures() != 0 {
		t.Fatalf("failures should reset, got %d", cb.Failures())
	}
}

func TestCircuit_DefaultsApplied(t *testing.T) {
	cb := NewCircuitBreaker(0, 0)
	if cb.failureThreshold != 5 {
		t.Fatalf("default threshold missing: %d", cb.failureThreshold)
	}
	if cb.cooldown != 30*time.Second {
		t.Fatalf("default cooldown missing: %v", cb.cooldown)
	}
}

func TestCircuit_StateStrings(t *testing.T) {
	tests := []struct {
		s    CircuitState
		want string
	}{
		{CircuitClosed, "closed"},
		{CircuitOpen, "open"},
		{CircuitHalfOpen, "half_open"},
		{CircuitState(99), "unknown"},
	}
	for _, tt := range tests {
		if tt.s.String() != tt.want {
			t.Errorf("%v.String() = %q, want %q", tt.s, tt.s.String(), tt.want)
		}
	}
}
