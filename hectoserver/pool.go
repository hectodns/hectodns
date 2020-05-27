package hectoserver

import (
	"context"
	"sync/atomic"
)

type ConnPool struct {
	Size int
	New  func() *Conn

	conns  []*Conn
	pos    int64
	maxpos int64
}

func (pool *ConnPool) Serve(ctx context.Context) error {
	for i := 0; i < pool.Size; i++ {
		conn[i] = pool.New()

		if err := conn[i].Serve(ctx); err != nil {
			return err
		}
	}

	pool.maxpos = int64(len(conn))
	return nil
}

func (pool *ConnPool) Handle(ctx context.Context, req *Request) (*Response, error) {
	no := atomic.AddInt64(&pool.curr, 1)
	atomic.CompareAndSwapInt64(&pool.curr, pool.maxpos, 0)

	// There is no guarantee, that current position does not exceed
	// maxpos, use modulo in order to ensure.
	no = no % pool.maxpos

	// Connection is thread-safe, therefore.
	conn := pool.conns[no]

	return conn.Handle(ctx, req)
}
