package hectoserver

import (
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miekg/dns"
	"github.com/pkg/errors"
)

const (
	// DefaultMaxIdleRequests is a number of maximum unprocessed requests.
	DefaultMaxIdleRequests = 1000
)

type request struct {
	id   uint16
	body []byte
	c    chan<- dns.Msg

	// at is used to keep track of the dangling request, which are not
	// processed and terminated by timeout.
	at time.Time
}

type Resolver struct {
	// Procname is the name of the process to start.
	Procname string

	// Procopts is a list of process options.
	Procopts []string

	// Timeout is a maximum duration before timing out the processing
	// of the single request.
	Timeout time.Duration

	// MaxIdleRequests is the maximum requests waiting for processing,
	// when stet to zero, defaults to DefaultMaxIdleRequests.
	MaxIdleRequests int

	// started is used to keep track of resolver state.
	started int64

	proc *os.Process

	rmap map[uint16]request
	rmu  sync.RWMutex

	sendC chan request
}

func (rsl *Resolver) Serve(ctx context.Context) (err error) {
	if !atomic.CompareAndSwapInt64(&rsl.started, 0, 1) {
		return ErrStarted
	}

	inrd, inwr, err := os.Pipe()
	if err != nil {
		return err
	}

	outrd, outwr, err := os.Pipe()
	if err != nil {
		defer func() {
			e := closeall(inrd, inwr)
			err = infer(err, e)
		}()
		return
	}

	proc, err := os.StartProcess(rsl.Procname, rsl.Procopts, &os.ProcAttr{
		Files: []*os.File{inrd, outwr, os.Stderr},
	})
	if err != nil {
		// Cleanup all acquired resources since the process start failed.
		// Infer final error from multiple errors.
		defer func() {
			e := closeall(inrd, inwr, outrd, outwr)
			err = infer(err, e)
		}()
		return
	}

	log.Printf("started resolver %s %s", rsl.Procname, rsl.Procopts)

	defer func() {
		err = infer(err, closeall(inrd, outwr))
	}()

	maxIdleRequests := rsl.MaxIdleRequests
	if maxIdleRequests <= 0 {
		maxIdleRequests = DefaultMaxIdleRequests
	}

	rsl.proc = proc
	rsl.rmap = make(map[uint16]request, maxIdleRequests)
	rsl.sendC = make(chan request, maxIdleRequests)

	go rsl.writer(ctx, inwr)
	go rsl.reader(ctx, outrd)

	return
}

// writer is a goroutine that writes requets to the pipe connected
// to the previously spawned process in order to handle DNS requests.
func (rsl *Resolver) writer(ctx context.Context, wr io.WriteCloser) (err error) {
	defer func() {
		err = infer(err, wr.Close())
	}()

	for {
		select {
		case req := <-rsl.sendC:
			rsl.rmu.Lock()
			rsl.rmap[req.id] = req
			rsl.rmu.Unlock()

			_, err = wr.Write(req.body)
			if err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// reader is a goroutine that reades responses (not necessarily in order)
// from the resolver process.
func (rsl *Resolver) reader(ctx context.Context, rd io.ReadCloser) (err error) {
	defer func() {
		err = infer(err, rd.Close())
	}()

	for {
		var resp Message
		_, err = resp.ReadFrom(rd)
		if err != nil {
			return
		}

		rsl.rmu.Lock()
		req, ok := rsl.rmap[resp.Body.Id]
		if ok {
			delete(rsl.rmap, resp.Body.Id)
		}
		rsl.rmu.Unlock()

		if !ok {
			continue
		}

		// Write response and close the channel after to release resources.
		req.c <- resp.Body
		close(req.c)
	}
}

// serve is a blocking call that serves a single DNS request.
func (rsl *Resolver) serve(dnsreq *dns.Msg) (*dns.Msg, error) {
	var buf bytes.Buffer

	// Serialize the DNS request in the serving goroutine to release
	// writer goroutine resources as much as possible.
	req := Message{Body: *dnsreq}
	if _, err := req.WriteTo(&buf); err != nil {
		return nil, err
	}

	// Create a buffered channel in order to prevent locking of the
	// reader goroutine on submission of the response.
	respC := make(chan dns.Msg, 1)
	rsl.sendC <- request{id: dnsreq.Id, body: buf.Bytes(), c: respC}

	// When timeout is not set, lock on reading the message forever.
	if rsl.Timeout == 0 {
		resp := <-respC
		return &resp, nil
	}

	timer := time.NewTimer(rsl.Timeout)
	defer timer.Stop()

	select {
	case resp := <-respC:
		return &resp, nil
	case <-timer.C:
		return nil, ErrTimeout
	}
}

// ServeDNS is a DNS handler implementation.
func (rsl *Resolver) ServeDNS(w dns.ResponseWriter, req *dns.Msg) {
	resp, err := rsl.serve(req)
	if err != nil {
		panic(err)
	}

	w.WriteMsg(resp)
}

// infer returns errors grouped into a single error.
func infer(errs ...error) (err error) {
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
		err = infer(err, c.Close())
	}
	return
}
