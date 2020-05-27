package hectoserver

import (
	"bufio"
	"context"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

const (
	// DefaultMaxIdleRequests is a number of maximum unprocessed requests.
	DefaultMaxIdleRequests = 1024
)

type connRW struct {
	resp *Response
}

func (crw *connRW) Write(resp *Response) error {
	crw.resp = resp
	return nil
}

type pub struct {
	req *Request
	rw  ResponseWriter
	C   chan<- struct{}
}

type Conn struct {
	// Procname is the name of the process to start.
	Procname string

	// Procopts is a list of process options.
	Procopts []string

	// MaxIdleRequests is the maximum requests waiting for processing,
	// when stet to zero, no idle requests are allowed.
	MaxIdleRequests int

	// started is used to keep track of resolver state.
	started int64

	proc *os.Process

	pubmap map[int64]pub
	pubmu  sync.RWMutex

	sendC chan pub
}

func (conn *Conn) Serve(ctx context.Context) (err error) {
	if !atomic.CompareAndSwapInt64(&conn.started, 0, 1) {
		return ErrStarted
	}

	var inrd, outrd, inwr, outwr *os.File

	// Ensure that all resources are released after the error.
	defer func() {
		// These file are never used, therefore they should be
		// closed unconditionally.
		err1 := closeall(inrd, outwr)
		err = checkerr(err, err1)

		// When the error occured at the previous step of unused
		// resources release, release all resources and free up the rest.
		if err != nil {
			err1 = closeall(inwr, outrd)
		}

		err = checkerr(err, err1)
	}()

	inrd, inwr, err = os.Pipe()
	if err != nil {
		return err
	}
	outrd, outwr, err = os.Pipe()
	if err != nil {
		return
	}

	proc, err := os.StartProcess(conn.Procname, conn.Procopts, &os.ProcAttr{
		Files: []*os.File{inrd, outwr, os.Stderr},
	})
	if err != nil {
		return
	}

	log.Printf("started resolver %s %s", conn.Procname, conn.Procopts)

	maxIdleRequests := conn.MaxIdleRequests
	if maxIdleRequests <= 0 {
		maxIdleRequests = DefaultMaxIdleRequests
	}

	conn.proc = proc
	conn.pubmap = make(map[int64]pub, maxIdleRequests)
	conn.sendC = make(chan pub, maxIdleRequests)

	go conn.writer(ctx, inwr)
	go conn.reader(ctx, outrd)

	return
}

// writer is a goroutine that writes requets to the pipe connected
// to the previously spawned process in order to handle requests.
func (conn *Conn) writer(ctx context.Context, wr io.WriteCloser) (err error) {
	defer func() {
		err = checkerr(err, wr.Close())
	}()

	for {
		select {
		case pub := <-conn.sendC:
			conn.pubmu.Lock()
			conn.pubmap[pub.req.ID] = pub
			conn.pubmu.Unlock()

			err = pub.req.Write(wr)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// reader is a goroutine that reades responses (not necessarily in order)
// from the resolver process.
func (conn *Conn) reader(ctx context.Context, rd io.ReadCloser) (err error) {
	defer func() {
		err = checkerr(err, rd.Close())
	}()

	bufrd := bufio.NewReader(rd)

	for {
		resp, err := ReadResponse(bufrd)
		if err != nil {
			return err
		}

		conn.pubmu.Lock()
		req, ok := conn.pubmap[resp.ID]
		if ok {
			delete(conn.pubmap, resp.ID)
		}
		conn.pubmu.Unlock()

		if !ok {
			// Simply ingore unrecognized response.
			continue
		}

		// Write response and close the channel after to release resources.
		req.C <- struct{}{}
		req.rw.Write(resp)
		close(req.C)
	}
}

// Handle implements Handler interface.
//
// The method submits the request through the pipe to the child
// process and waits for response.
func (conn *Conn) Handle(ctx context.Context, req *Request) (*Response, error) {
	var rw connRW

	// Create a buffered channel in order to prevent locking of the
	// reader goroutine on submission of the response.
	respC := make(chan struct{}, 1)
	conn.sendC <- pub{req: req, rw: &rw, C: respC}

	select {
	case <-respC:
		return rw.resp, nil
	case <-ctx.Done():
		return nil, ErrTimeout
	}
}

// checkerr returns errors grouped into a single error.
func checkerr(errs ...error) (err error) {
	for _, e := range errs {
		if err == nil {
			err = e
			continue
		}
		err = errors.WithMessage(err, e.Error())
	}
	return
}

// closeall closes all passed Closers sequentially.
func closeall(closers ...io.Closer) (err error) {
	for _, c := range closers {
		if c != nil {
			err = checkerr(err, c.Close())
		}
	}
	return
}
