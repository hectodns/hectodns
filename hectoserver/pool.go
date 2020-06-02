package hectoserver

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
)

type ConnPool struct {
	// Cap is the capacity of the connection pool, or the number of
	// connections in the pool.
	Cap int

	// New specifies a function to generate a new connection to start
	// multiple clones of the same connection.
	New func() *Conn

	mu    sync.RWMutex
	conns []*Conn
	ctx   context.Context

	pos int32
	cap int32
}

func (pool *ConnPool) Serve(ctx context.Context) error {
	conns := make([]*Conn, pool.Cap)
	logger := zerolog.Ctx(ctx)

	for i := 0; i < pool.Cap; i++ {
		conn := pool.New()

		log := logger.With().Str("name", conn.Procname).Int("process", i).Logger()
		ctx := log.WithContext(ctx)

		if err := conn.Serve(ctx); err != nil {
			return err
		}

		conns[i] = conn
	}

	pool.conns = conns
	pool.cap = int32(pool.Cap)
	pool.ctx = ctx
	return nil
}

func (pool *ConnPool) Handle(ctx context.Context, req *Request) (*Response, error) {
	// There is no guarantee, that current position does not exceed
	// cap, use modulo in order to ensure this.
	pos := atomic.AddInt32(&pool.pos, 1)
	pos = pos % int32(pool.cap)

	// Connection is thread-safe, therefore spawn processing as it is.
	pool.mu.RLock()
	conn := pool.conns[pos]
	pool.mu.RUnlock()

	// Attempt to handle the incoming request, and when the connection
	// was closed for some reason, attempt to restart the connection,
	// but leave the request unprocessed for the sake of performance.
	resp, err := conn.Handle(ctx, req)
	if err != ErrConnClosed {
		return resp, err
	}

	// Restart connection, when it's closed, multiple concurrent requests
	// could attempt to restart the closed connection, but only one will
	// succeed.
	pool.mu.Lock()
	conn = pool.New()
	err = conn.Serve(pool.ctx)
	pool.conns[pos] = conn
	pool.mu.Unlock()

	if err != nil && err != ErrConnStarted {
		return nil, err
	}
	return nil, ErrConnClosed
}
