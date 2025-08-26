package embeddednats

import (
	"context"
	"fmt"
	"net"
	"time"

	nserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Server is a tiny façade around a nats-server instance suitable for embedding.
type Server struct {
	s *nserver.Server
}

func New(opts *nserver.Options) (*Server, error) {
	ns, err := nserver.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("nats server create: %w", err)
	}
	return &Server{s: ns}, nil
}

// Start launches the server in its own goroutine.
func (e *Server) Start() { go e.s.Start() }

// ClientURL returns the nats:// URL clients should connect to.
func (e *Server) ClientURL() string { return e.s.ClientURL() }

// Ready blocks until the server is ready or the context expires.
func (e *Server) Ready(ctx context.Context) error {
	// Instead of relying on ReadyForConnections, try to actually connect
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			// Try to connect as a client to verify the server is truly ready
			if e.canConnect() {
				return nil
			}
		}
	}
}

// canConnect attempts a quick connection to verify server readiness
func (e *Server) canConnect() bool {
	nc, err := nats.Connect(e.s.ClientURL(), nats.Timeout(100*time.Millisecond))
	if err != nil {
		return false
	}
	nc.Close()
	return true
}

// ShutdownAndWait signals the server to stop and waits up to maxWait for it.
func (e *Server) ShutdownAndWait(ctx context.Context, maxWait time.Duration) error {
	e.s.Shutdown()
	wait := make(chan struct{}, 1)
	go func() { e.s.WaitForShutdown(); wait <- struct{}{} }()
	select {
	case <-wait:
		return nil
	case <-time.After(maxWait):
		return fmt.Errorf("server wait timeout after %s", maxWait)
	case <-ctx.Done():
		return fmt.Errorf("server wait canceled: %w", ctx.Err())
	}
}

// Port returns the bound TCP port or 0 if unknown.
func (e *Server) Port() int {
	if a := e.s.Addr(); a != nil {
		if ta, ok := a.(*net.TCPAddr); ok {
			return ta.Port
		}
	}
	return 0
}
