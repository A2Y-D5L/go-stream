package client

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/a2y-d5l/go-stream/topic"
)

// Healthy returns an error if the stream is not in a healthy state. This is a
// lightweight check that verifies the basic operational status of the stream.
// For a more thorough check including server readiness, use DeepHealthCheck().
func (s *Stream) Healthy(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	switch {
	case !s.started.Load():
		return fmt.Errorf("%w: stream not started", ErrStreamUnhealthy)
	case s.closed.Load():
		return fmt.Errorf("%w: stream already closed", ErrStreamUnhealthy)
	case s.nc == nil:
		return fmt.Errorf("%w: client not initialized", ErrStreamUnhealthy)
	case s.srv == nil:
		return fmt.Errorf("%w: server not initialized", ErrStreamUnhealthy)
	case s.js == nil && s.cfg.DefaultTopicMode == topic.TopicModeJetStream:
		return fmt.Errorf("%w: durable stream not initialized", ErrStreamUnhealthy)
	case s.nc.Status() != nats.CONNECTED:
		return fmt.Errorf("%w: client not connected", ErrStreamUnhealthy)
	default:
		// For high-load scenarios, if the client is connected, assume the server is ready.
		// This avoids expensive server readiness checks on every publish operation.
		// If there are actual server issues, they will manifest as client connection problems.
		return nil
	}
}

// DeepHealthCheck performs a thorough health check including server readiness verification.
// This is more expensive than Healthy() and should be used sparingly.
func (s *Stream) DeepHealthCheck(ctx context.Context) error {
	// First do the basic health check
	if err := s.Healthy(ctx); err != nil {
		return err
	}

	s.mu.RLock()
	srv := s.srv
	s.mu.RUnlock()

	// Now do the expensive server readiness check
	if srv != nil {
		ctx, cancel := context.WithTimeout(ctx, s.cfg.ServerReadyTimeout)
		defer cancel()

		if err := srv.Ready(ctx); err != nil {
			return fmt.Errorf("%w: server not ready", ErrStreamUnhealthy)
		}
	}

	return nil
}

// Close drains the client and shuts down the server gracefully.
func (s *Stream) Close(ctx context.Context) error {
	if !s.started.Load() || !s.closed.CompareAndSwap(false, true) {
		return ErrStreamClosed
	}

	// Copy current state to local variables to avoid holding the lock
	// while waiting for the client to drain or server to shutdown.
	// This is important to avoid deadlocks if the client or server
	// tries to acquire the same lock.
	// It also allows the caller to call Close multiple times safely.
	// If the stream is already closed, we can return early.
	if !s.started.Load() {
		return ErrStreamClosed
	}

	s.log.Info("closing stream...")

	s.mu.Lock()
	nc := s.nc
	srv := s.srv
	drainTO := s.cfg.DrainTimeout
	maxWait := s.cfg.ServerShutdownMaxWait
	s.mu.Unlock()

	var merr multiErr

	if nc != nil {
		done := make(chan error, 1)
		go func() { done <- nc.Drain() }()
		select {
		case err := <-done:
			if err != nil {
				merr.add(fmt.Errorf("nats drain: %w", err))
			}
		case <-time.After(drainTO):
			merr.add(fmt.Errorf("nats drain timeout after %s", drainTO))
			nc.Close()
		case <-ctx.Done():
			merr.add(fmt.Errorf("nats drain canceled: %w", ctx.Err()))
			nc.Close()
		}
	}

	if srv != nil {
		if err := srv.ShutdownAndWait(ctx, maxWait); err != nil {
			merr.add(err)
		}
	}

	s.mu.Lock()
	s.nc, s.srv, s.js = nil, nil, nil
	s.mu.Unlock()

	if len(merr) > 0 {
		return merr
	}

	s.log.Info("stream closed.")

	return nil
}

// multiErr accumulates multiple errors.
type multiErr []error

func (m *multiErr) add(err error) { *m = append(*m, err) }

func (m multiErr) Error() string {
	if len(m) == 0 {
		return "no errors"
	}
	if len(m) == 1 {
		return m[0].Error()
	}
	var msg = fmt.Sprintf("%d errors: %s", len(m), m[0].Error())
	for _, e := range m[1:] {
		msg += "; " + e.Error()
	}
	return msg
}
