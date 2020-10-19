package lookup

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/hectodns/hectodns/ns"

	"github.com/rs/zerolog"
	"github.com/zclconf/go-cty/cty/json"
)

type Server struct {
	ns.Handler
	Shutdown ns.Shutdowner
	Close    ns.Shutdowner
}

// CreateAndServe creates new connections to the specifies resolvers,
// and returns handler instance to bypass them a single DNS request.
//
// Method returns Shutdown and Close functions to control lifetime of the
// handler, after calling it, handler will return an error on attempt to
// process a new request.
func CreateAndServe(ctx context.Context, config *ns.ServerConfig) (s Server, err error) {
	var (
		handlers    = make([]ns.Handler, len(config.Resolvers))
		shutdowners = make([]ns.Shutdowner, len(config.Resolvers))
		closers     = make([]ns.Shutdowner, len(config.Resolvers))
	)

	defer func() {
		s.Shutdown = ns.MultiShutdown(shutdowners...)
		s.Close = ns.MultiShutdown(closers...)
		s.Handler = ns.MultiHandler(handlers...)
	}()

	for i, r := range config.Resolvers {
		r := r

		var (
			b   []byte
			err error
		)

		if op := r.Options; !op.IsNull() {
			b, err = json.Marshal(op, op.Type())
			if err != nil {
				return s, err
			}
		}

		newConn := func() *Conn {
			return &Conn{
				Root:            config.ResolverDirectory,
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
			return s, err
		}

		handlers[i] = pool
		shutdowners[i] = ns.ShutdownFunc(pool.Shutdown)
		closers[i] = ns.ShutdownFunc(pool.Close)
	}

	return s, nil
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

func (pool *ConnPool) Close(ctx context.Context) error {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	fn := make([]ns.Shutdowner, len(pool.conns))
	for i, conn := range pool.conns {
		fn[i] = ns.ShutdownFunc(conn.Close)
	}

	return ns.ShutdownAll(ctx, fn...)
}

func (pool *ConnPool) Shutdown(ctx context.Context) error {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	fn := make([]ns.Shutdowner, len(pool.conns))
	for i, conn := range pool.conns {
		fn[i] = ns.ShutdownFunc(conn.Shutdown)
	}

	return ns.ShutdownAll(ctx, fn...)
}

func (pool *ConnPool) Handle(ctx context.Context, req *ns.Request) (*ns.Response, error) {
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
