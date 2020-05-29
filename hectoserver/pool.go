package hectoserver

import (
	"context"
	"log"
	"sync/atomic"
)

type ConnPool struct {
	// Cap is the capacity of the connection pool, or the number of
	// connections in the pool.
	Cap int

	// New specifies a function to generate a new connection to start
	// multiple clones of the same connection.
	New func() *Conn

	conns []*Conn
	cap   int64
}

func (pool *ConnPool) Serve(ctx context.Context) error {
	conns := make([]*Conn, pool.Cap)

	for i := 0; i < pool.Cap; i++ {
		conn := pool.New()
		if err := conn.Serve(ctx); err != nil {
			return err
		}

		conns[i] = conn
		log.Printf("started resolver %q-%d", conns[i].Procname, i)
	}

	pool.conns = conns
	pool.cap = int64(len(pool.conns))
	return nil
}

func (pool *ConnPool) Handle(ctx context.Context, req *Request) (*Response, error) {
	// Allow pos to overflow.
	pos := atomic.AddInt64(&pool.cap, 1)

	// There is no guarantee, that current position does not exceed
	// cap, use modulo in order to ensure.
	pos = pos % pool.cap

	// Connection is thread-safe, therefore spawn processing as it is.
	conn := pool.conns[pos]
	return conn.Handle(ctx, req)
}
