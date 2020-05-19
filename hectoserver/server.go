package hectoserver

import (
	"context"
	"log"

	"github.com/miekg/dns"
	"github.com/pkg/errors"
)

// Error represents a server error.
type Error struct{ err string }

// Error returns string representation of the error.
func (e Error) Error() string { return "hectodns: " + e.err }

var (
	// ErrTimeout is returned when request processing did not served
	// within a configured duration.
	ErrTimeout = Error{err: "request processing timeout"}

	// ErrStarted is returned on attempt to start resolver that is
	// already started.
	ErrStarted = Error{err: "already started"}
)

type responseWriter struct {
	dns.ResponseWriter
	msg *dns.Msg
}

func (rw responseWriter) Write([]byte) (int, error) {
	return 0, errors.New("not supported")
}

func (rw responseWriter) WriteMsg(msg *dns.Msg) error {
	rw.msg = msg
	return nil
}

type MultiResolver struct {
	rr []*Resolver
}

func NewMultiResolver(cc []ResolverConfig) *MultiResolver {
	mr := MultiResolver{
		rr: make([]*Resolver, len(cc)),
	}
	for i, c := range cc {
		mr.rr[i] = &Resolver{
			Procname: c.Name,
			//Procopts: *c.Options,
		}
	}
	return &mr
}

func (mr *MultiResolver) Serve(ctx context.Context) error {
	for _, r := range mr.rr {
		if err := r.Serve(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (mr *MultiResolver) ServeDNS(w dns.ResponseWriter, req *dns.Msg) {
	rw := responseWriter{w, nil}

	for i, r := range mr.rr {
		r.ServeDNS(rw, req)
		log.Printf("%d processed", i)

		if rw.msg != nil && rw.msg.Rcode != dns.RcodeRefused {
			w.WriteMsg(rw.msg)
			return
		}
	}

	var resp dns.Msg
	resp.SetRcode(req, dns.RcodeServerFailure)

	w.WriteMsg(&resp)
}
