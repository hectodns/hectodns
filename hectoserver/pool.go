package hectoserver

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/zclconf/go-cty/cty/json"
)

// ShutdownFunc tells a handler to terminate its work. ShutdownFunc
// waits for the work to stop. A ShutdownFunc may be called by multiple
// goroutines simultaneously.
type ShutdownFunc func() error

// ShutdownAll executes all passed ShutdownFunc sequentially.
func ShutdownAll(fn ...ShutdownFunc) (err error) {
	for _, f := range fn {
		if f != nil {
			err = checkerr(err, f())
		}
	}
	return
}

// CreateAndServe creates new connections to the specifies resolvers,
// and returns handler instance to bypass them a single DNS request.
//
// Method returns ShutdownFunc to control lifetime of the handlers,
// after calling it, handler will return an error on attempt to process
// a request.
func CreateAndServe(ctx context.Context, config *ServerConfig) (Handler, ShutdownFunc, error) {
	handlers := make([]Handler, len(config.Resolvers))
	closers := make([]io.Closer, len(config.Resolvers))

	// Return a function to control the lifetime of the handlers.
	shutdownFunc := func() error { return closeall(closers...) }

	for i, r := range config.Resolvers {
		r := r
		b, err := json.Marshal(r.Options, r.Options.Type())
		if err != nil {
			return nil, nil, err
		}

		newConn := func() *Conn {
			return &Conn{
				Root:            config.Root,
				Procname:        r.Name,
				Procenv:         string(b),
				MaxIdleRequests: r.MaxIdle,
			}
		}

		// Ensure that one process starts when the configured number
		// of processes is not defined. (it's either 0 or not set).
		poolCap := r.Processes
		if poolCap < 1 {
			poolCap = 1
		}

		pool := &ConnPool{Cap: poolCap, New: newConn}
		err = pool.Serve(ctx)
		if err != nil {
			return nil, shutdownFunc, err
		}

		handlers[i] = pool
		closers[i] = pool
	}

	return MultiHandler(handlers...), shutdownFunc, nil
}

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

		log := logger.With().Str("name", conn.Procname).Int("proc", i).Logger()
		ctx := log.WithContext(ctx)

		if err := conn.Serve(ctx); err != nil {
			return err
		}

		conns[i] = conn
	}

	logger.Info().Msgf("started %d resolvers in pool", pool.Cap)

	pool.conns = conns
	pool.cap = int32(pool.Cap)
	pool.ctx = ctx
	return nil
}

func (pool *ConnPool) Close() error {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	for _, c := range pool.conns {
		c.Close()
	}
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
