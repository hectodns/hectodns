package hectoserver

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	StateNew:     "new",
	StateStarted: "started",
	StateClosed:  "closed",
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
	Procenv string

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

func (conn *Conn) String() string {
	return conn.Procname
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
		conn.log().Debug().Msgf("conn transition from '%s' to '%s'", from, to)
		if cb := conn.ConnState; cb != nil {
			cb(conn, to)
		}
	}
	return
}

func (conn *Conn) forkexec() (proc *os.Process, r, w, e *os.File, err error) {
	var stdin, stdout, stderr *os.File

	// Close unnecessary files, ingore errors.
	defer func() {
		try(stdin.Close, stdout.Close)
	}()

	defer func() {
		if err != nil {
			try(r.Close, w.Close)
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
	e, stderr, err = os.Pipe()
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

	// Ensure the first argument is the name of the program. Go library
	// does not put it for us, we have to do int manually.
	argv = append([]string{name}, conn.Procopts...)

	proc, err = os.StartProcess(name, argv, &os.ProcAttr{
		Files: []*os.File{stdin, stdout, stderr},
		Env:   append(os.Environ(), "hectodns.options="+conn.Procenv),
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

	proc, r, w, e, err := conn.forkexec()
	if err != nil {
		return
	}

	log := conn.logger.With().Int("pid", proc.Pid).Logger()
	conn.logger = &log

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
	go conn.erroer(ctx, e)

	return
}

type pub struct {
	req   *Request
	C     chan<- *Response
	waitC chan struct{}
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
	reader := chanReader{
		Read: func(bufr *bufio.Reader) chanResponse {
			resp, err := ReadResponse(bufr)
			return chanResponse{resp, err}
		},
	}

	for {
		select {
		case chanresp := <-reader.C(rd):
			if chanresp.err != nil {
				return err
			}

			resp := chanresp.ret.(*Response)

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
			close(pub.waitC)

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

const (
	levelDebug = "d"
	levelInfo  = "i"
	levelWarn  = "w"
	levelError = "e"
)

// erroer is a goroutine that reads lines from stderr of the connection
// and puts them into the log.
func (conn *Conn) erroer(ctx context.Context, rd io.ReadCloser) (err error) {
	defer rd.Close()

	defer conn.close(false)

	reader := chanReader{
		Read: func(bufr *bufio.Reader) chanResponse {
			s, err := bufr.ReadString('\n')
			return chanResponse{s, err}
		},
	}

	log := conn.log().With().Bool("captured", true).Logger()
	elog := log.With().Bool("malformed", true).Logger()

	for {
		select {
		case chanresp := <-reader.C(rd):
			if err := chanresp.err; err != nil {
				return err
			}

			text := strings.TrimRight(chanresp.ret.(string), "\n")
			parts := strings.SplitN(text, ":", 2)
			if len(parts) != 2 {
				elog.Warn().Msg(text)
				continue
			}

			switch level, msg := parts[0], parts[1]; level {
			case levelDebug:
				log.Debug().Msg(msg)
			case levelInfo:
				log.Info().Msg(msg)
			case levelWarn:
				log.Warn().Msg(msg)
			case levelError:
				log.Error().Msg(msg)
			default:
				elog.Warn().Msg(text)
			}
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

	// Close the incoming requests queue, so only enqueued requests can
	// be processed.
	close(conn.sendC)

	// When the shutdown is graceful, wait until queue is empty.
	if graceful {
		conn.pubmu.Lock()

		// Copy the list of response channels, wait until each of them is
		// closed, which indicates the completion of request processing.
		chans := make([]chan struct{}, 0, len(conn.pubmap))
		for _, pub := range conn.pubmap {
			chans = append(chans, pub.waitC)
		}

		conn.pubmu.Unlock()

		conn.logger.Debug().Msgf("waiting for comletion of %d requests", len(chans))
		for _, ch := range chans {
			<-ch
		}
	}

	conn.proc.Kill()
	conn.proc.Wait()

	conn.log().Debug().Msg("process killed")

	// Terminate the writer/reader and errorer goroutines.
	close(conn.stopC)

	conn.pubmap = nil
	conn.proc = nil
}

// Shutdown waits for completion of unprocessed requests.
//
// Once Shutdown has been called on a connection, it may not be reused; future
// calls to methods such as Handle will return ErrConnClosed.
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
	conn.sendC <- pub{req: req, C: respC, waitC: make(chan struct{})}

	// Increment the number of requests submitted for processing.
	atomic.AddInt32(&conn.pubs, 1)

	select {
	case resp := <-respC:
		// Decrement number of unprocessed requests.
		atomic.AddInt32(&conn.pubs, -1)
		return resp, nil
	case <-conn.stopC:
		return nil, ErrConnClosed
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

func try(fns ...func() error) (err error) {
	for _, fn := range fns {
		if fn != nil {
			err = checkerr(err, fn())
		}
	}
	return
}
