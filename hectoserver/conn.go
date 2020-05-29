package hectoserver

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

// ConnState represents the state of a client connection to a server.
// It is used by optional ConnState hook.
type ConnState int32

const (
	// StateNew represents a new process connection that is expected to
	// receive a request immediately. Connections begins at this state
	// and transition to either StateActive or StateClosed.
	StateNew ConnState = iota

	// StateStarted represents a connection that is ready to process
	// requests from clients.
	StateStarted

	// StateClosed represents a closed connection. This is a
	// terminal state.
	StateClosed
)

func (c *ConnState) is(state ConnState) bool {
	return atomic.CompareAndSwapInt32((*int32)(c), int32(state), int32(state))
}

func (c *ConnState) transition(from, to ConnState) bool {
	return atomic.CompareAndSwapInt32((*int32)(c), int32(from), int32(to))
}

// String returns the string representation of the ConnState.
func (c ConnState) String() string {
	text, ok := connStateText[c]
	if !ok {
		return fmt.Sprintf("ConnState(%d)", c)
	}
	return text
}

var connStateText = map[ConnState]string{
	StateNew:     "StateNew",
	StateStarted: "StateStarted",
	StateClosed:  "StateClosed",
}

const (
	// DefaultMaxIdleRequests is a number of maximum unprocessed requests.
	DefaultMaxIdleRequests = 1024
)

type Conn struct {
	// Procname is the name of the process to start.
	Procname string

	// Procopts is a list of process options.
	Procopts []string

	// MaxIdleRequests is the maximum requests waiting for processing,
	// when stet to zero, no idle requests are allowed.
	MaxIdleRequests int

	// ConnState specifies an optional callback function that is called
	// when a connection changes state.
	ConnState func(*Conn, ConnState)

	// state is used to keep track of connection state.
	state ConnState

	proc   *os.Process
	procmu sync.Mutex

	pubmap map[int64]pub
	pubmu  sync.RWMutex

	// The pubs stores the count of clients requests. This value
	// is incremented on each new request and decremented on each
	// returned response.
	pubs int32

	sendC chan pub
}

func (conn *Conn) setState(from, to ConnState) (ok bool) {
	ok = conn.state.transition(from, to)
	if cb := conn.ConnState; cb != nil && ok {
		cb(conn, to)
	}
	return
}

func (conn *Conn) forkexec() (proc *os.Process, r, w *os.File, err error) {
	var stdin, stdout *os.File

	// Close unnecessary files, ingore errors.
	defer func() {
		closeall(stdin, stdout)
	}()

	defer func() {
		if err != nil {
			closeall(r, w)
		}
	}()

	stdin, w, err = os.Pipe()
	if err != nil {
		return
	}
	r, stdout, err = os.Pipe()
	if err != nil {
		return
	}

	name, argv := conn.Procname, conn.Procopts
	proc, err = os.StartProcess(name, argv, &os.ProcAttr{
		Files: []*os.File{stdin, stdout, os.Stderr},
	})

	return
}

// ErrConnStarted is returned by Conn's Server method on attempt to
// start resolver that is already started.
var ErrConnStarted = Error{err: "already started"}

func (conn *Conn) Serve(ctx context.Context) (err error) {
	// Trigger the ConnState callback on the StateNew.
	conn.setState(StateNew, StateNew)

	defer func() {
		if err == nil {
			conn.setState(StateNew, StateStarted)
		}
	}()

	// The lock is necessary to protect resources, since conn
	// state will be changed only at the end of this function
	// to correctly specify the moment of state transition.
	conn.procmu.Lock()
	defer conn.procmu.Unlock()

	if conn.state.is(StateStarted) {
		return ErrConnStarted
	}

	proc, r, w, err := conn.forkexec()
	if err != nil {
		return
	}

	maxidle := conn.MaxIdleRequests
	if maxidle <= 0 {
		maxidle = DefaultMaxIdleRequests
	}

	conn.proc = proc
	conn.pubmap = make(map[int64]pub, maxidle)
	conn.sendC = make(chan pub, maxidle)

	go conn.writer(ctx, w)
	go conn.reader(ctx, r)

	return
}

type pub struct {
	req *Request
	rw  ResponseWriter
	C   chan<- struct{}
}

// writer is a goroutine that writes requets to the pipe connected
// to the previously spawned process in order to handle requests.
func (conn *Conn) writer(ctx context.Context, wr io.WriteCloser) (err error) {
	defer func() {
		err = checkerr(err, wr.Close())
		conn.shutdown()
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
		conn.shutdown()
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

		atomic.AddInt32(&conn.pubs, -1)
	}
}

func (conn *Conn) shutdown() {
	conn.setState(StateStarted, StateClosed)
}

func (conn *Conn) Shutdown() error {
	return nil
}

func (conn *Conn) Close() error {
	return nil
}

// ErrConnClosed is returned by the Conn's Handle method after a call
// to Shutdown or Close method.
var ErrConnClosed = Error{err: "connection closed"}

type connRW struct {
	resp *Response
}

func (crw *connRW) Write(resp *Response) error {
	crw.resp = resp
	return nil
}

// Handle implements Handler interface.
//
// The method submits the request through the pipe to the child
// process and waits for response.
//
// After Shutdown or Close, the returned error is ErrConnClosed.
func (conn *Conn) Handle(ctx context.Context, req *Request) (*Response, error) {
	var rw connRW

	if !conn.state.is(StateStarted) {
		return nil, ErrConnClosed
	}
	println("HANDLE!!!!")

	// Create a buffered channel in order to prevent locking of the
	// reader goroutine on submission of the response.
	respC := make(chan struct{}, 1)
	conn.sendC <- pub{req: req, rw: &rw, C: respC}

	// Increment the number of requests submitted for processing.
	atomic.AddInt32(&conn.pubs, 1)

	select {
	case <-respC:
		// Decrement number of unprocessed requests.
		atomic.AddInt32(&conn.pubs, -1)
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
		if e == nil {
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
