package hectoserver

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
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
	Root string

	// Procname is the name of the process to start.
	Procname string

	// Procopts is a list of process arguments.
	Procopts []string

	// Procenv is a process environment variables.
	Procenv map[string]string

	// MaxIdleRequests is the maximum requests waiting for processing,
	// when stet to zero, no idle requests are allowed.
	MaxIdleRequests int

	// ConnState specifies an optional callback function that is called
	// when a connection changes state.
	ConnState func(*Conn, ConnState)

	// state is used to keep track of connection state.
	state  ConnState
	logger *zerolog.Logger

	proc   *os.Process
	procmu sync.Mutex

	pubmap map[int64]pub
	pubmu  sync.RWMutex

	// The pubs stores the count of clients requests. This value
	// is incremented on each new request and decremented on each
	// returned response.
	pubs int32

	stopC chan struct{}
	sendC chan pub
}

func (conn *Conn) log() *zerolog.Logger {
	if conn.logger == nil {
		logger := zerolog.Nop()
		conn.logger = &logger
	}
	return conn.logger
}

func (conn *Conn) setState(from, to ConnState) (ok bool) {
	ok = conn.state.transition(from, to)
	if ok {
		conn.log().Debug().Msgf("transitioning from '%s' to '%s'", from, to)
		if cb := conn.ConnState; cb != nil {
			cb(conn, to)
		}
	} else {
		conn.log().Debug().Msgf("failed transition from '%s' to '%s'", from, to)
	}
	return
}

func (conn *Conn) forkexec() (proc *os.Process, r, w *os.File, err error) {
	var stdin, stdout *os.File

	var env []string
	if procenv := conn.Procenv; procenv != nil {
		for k, v := range procenv {
			env = append(env, k+"="+v)
		}
	}

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

	// When the root is specified, search the executables for resolvers
	// exatly in the provided directory, otherwise use PATH.
	if root := conn.Root; root != "" {
		root, err = filepath.Abs(root)
		if err != nil {
			return
		}
		name = filepath.Join(root, name)
	}

	proc, err = os.StartProcess(name, argv, &os.ProcAttr{
		Files: []*os.File{stdin, stdout, os.Stderr},
		Env:   env,
	})

	return
}

// ErrConnStarted is returned by Conn's Server method on attempt to
// start resolver that is already started.
var ErrConnStarted = Error{err: "already started"}

// Serve starts a process with configured arguments, creates communication
// pipes to send and receive DNS requests over HTTP protocol.
//
// Use Handle method in order to process request. When the processing is
// done, terminate connection gracefully using Shutdown method or use
// Close to terminate connection immediately.
func (conn *Conn) Serve(ctx context.Context) (err error) {
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

	conn.logger = zerolog.Ctx(ctx)
	// Trigger the ConnState callback on the StateNew.
	conn.setState(StateNew, StateNew)

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
	conn.stopC = make(chan struct{}, 1)

	go conn.writer(ctx, w)
	go conn.reader(ctx, r)

	return
}

type pub struct {
	req *Request
	C   chan<- *Response
}

// writer is a goroutine that writes requets to the pipe connected
// to the previously spawned process in order to handle requests.
func (conn *Conn) writer(ctx context.Context, wr io.WriteCloser) (err error) {
	defer wr.Close()

	// Either on pipe error (failed attempt to write request to
	// the client, signal master goroutine to close the reader.
	defer conn.close(false)

	for {
		select {
		case pub, ok := <-conn.sendC:
			// Channel is closed and no more requests for this incarnation
			// will be available, exit without errors.
			if !ok {
				return nil
			}
			conn.pubmu.Lock()
			conn.pubmap[pub.req.ID] = pub
			conn.pubmu.Unlock()

			err = pub.req.Write(wr)
			if err != nil {
				return err
			}
		case <-conn.stopC:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

type chanResponse struct {
	resp *Response
	err  error
}

// The chanReader is a wrapper around connection reader used to
// create a channel of Responses by continuosly fetching responses
// from the connection.
type chanReader struct {
	bufr *bufio.Reader

	once sync.Once
	ch   chan chanResponse
}

// read starts and inifinite loop of reading the responses from
// the connection. When the read call returns an error, a submission
// channel closes and loop terminates.
func (r *chanReader) read() {
	for {
		resp, err := ReadResponse(r.bufr)
		r.ch <- chanResponse{resp, err}

		if err != nil {
			close(r.ch)
			return
		}
	}
}

// The start creates a receive channel to deliver responses to clients,
// spawns a new goroutine to read responses from the reader and send
// them through the channel.
func (r *chanReader) start() {
	r.ch = make(chan chanResponse, 1)
	go r.read()
}

// C returns a read-only channel of response plus error.
func (r *chanReader) C() <-chan chanResponse {
	r.once.Do(r.start)
	return r.ch
}

// reader is a goroutine that reades responses (not necessarily in order)
// from the resolver process.
func (conn *Conn) reader(ctx context.Context, rd io.ReadCloser) (err error) {
	defer rd.Close()

	// When the reader is closed due to pipe error (failed to read
	// response from the connection channel). Then this goroutine
	// must signal to the master goroutine to terminate the writer
	// as well.
	defer conn.close(false)

	// Channel reader starts a goroutine to read responses sequentially,
	// this entity is useful in conjunction with select statement.
	//
	// Closing the passed reader will cause EOF error being written to
	// the response channel.
	reader := chanReader{bufr: bufio.NewReader(rd)}

	for {
		select {
		case chanresp := <-reader.C():
			resp, err := chanresp.resp, chanresp.err
			if err != nil {
				return err
			}

			// Find a channel where to write the response, the request
			// and response identifiers must match in order to find the
			// original request.
			conn.pubmu.Lock()
			pub, ok := conn.pubmap[resp.ID]
			if ok {
				delete(conn.pubmap, resp.ID)
			}
			conn.pubmu.Unlock()

			if !ok {
				// Simply ingore unrecognized response.
				continue
			}

			pub.C <- resp
			close(pub.C)

		// Handle cancellations propagated from the Serve call, it's
		// likely such calls are initialited by the API users, therfore
		// terminate connections.
		case <-ctx.Done():
			return nil
		case <-conn.stopC:
			return nil
		}
	}
}

func (conn *Conn) close(graceful bool) {
	conn.procmu.Lock()
	defer conn.procmu.Unlock()

	// Try to transition the state of the connection to the closed,
	// when the connection is already in a closed state, exit.
	if !conn.setState(StateStarted, StateClosed) {
		return
	}

	conn.proc.Kill()
	conn.proc.Wait()

	// Terminate the goroutines at first, and only then close the
	// channel for processing requests.
	close(conn.stopC)
	close(conn.sendC)

	conn.pubmap = nil
	conn.proc = nil
}

func (conn *Conn) Shutdown() error {
	conn.close(true)
	return nil
}

// Close immediately kills the process all unprocessed requests will be
// removed. For a graceful shutdown, use Shutdown.
//
// Once Close has been called on a connection, it may not be reused; future
// calls to methods such as Handle will return ErrConnClosed.
func (conn *Conn) Close() error {
	conn.close(false)
	return nil
}

// ErrConnClosed is returned by the Conn's Handle method after a call
// to Shutdown or Close method.
var ErrConnClosed = Error{err: "connection closed"}

// Handle implements Handler interface.
//
// The method submits the request through the pipe to the child
// process and waits for response.
//
// After Shutdown or Close, the returned error is ErrConnClosed.
func (conn *Conn) Handle(ctx context.Context, req *Request) (*Response, error) {
	if !conn.state.is(StateStarted) {
		return nil, ErrConnClosed
	}

	// Create a buffered channel in order to prevent locking of the
	// reader goroutine on submission of the response.
	respC := make(chan *Response, 1)
	conn.sendC <- pub{req: req, C: respC}

	// Increment the number of requests submitted for processing.
	atomic.AddInt32(&conn.pubs, 1)

	select {
	case resp := <-respC:
		// Decrement number of unprocessed requests.
		atomic.AddInt32(&conn.pubs, -1)
		return resp, nil
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
