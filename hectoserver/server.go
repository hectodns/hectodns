package hectoserver

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/miekg/dns"
	"github.com/rs/zerolog"
)

const (
	httpMethod    = http.MethodPost
	httpURL       = "/dns-query"
	httpUserAgent = "HectoDNS"
)

// Error represents a server error.
type Error struct{ err string }

// Error returns string representation of the error.
func (e Error) Error() string { return "hectodns: " + e.err }

var (
	// ErrTimeout is returned when request processing did not served
	// within a configured duration.
	ErrTimeout = Error{err: "request processing timeout"}

	// errContentLength is returned on when content length is not
	// set in HTTP response headers.
	errContentLength = Error{err: "content length is not known"}
)

type Request struct {
	// ID is a unique identifier of the request.
	ID int64

	Body dns.Msg

	// At is a request submission timestamp.
	At time.Time
}

func NewRequest(dnsreq dns.Msg) Request {
	return Request{
		ID:   int64(dnsreq.Id),
		Body: dnsreq,
		At:   time.Now(),
	}
}

func (r *Request) Write(w io.Writer) error {
	b, err := r.Body.Pack()
	if err != nil {
		return err
	}

	httpreq, err := http.NewRequest(httpMethod, httpURL, bytes.NewBuffer(b))
	if err != nil {
		return err
	}

	httpreq.Header.Set("user-agent", httpUserAgent)
	return httpreq.Write(w)
}

type Response struct {
	ID int64

	// StatusCode is numerical status of the HTTP response, e.g. 200.
	StatusCode int

	// Header maps HTTP header keys to values. Keys in the map are
	// canonicalized (see http.CanonicalHeaderKey).
	Header http.Header

	// Body respresents DNS response.
	Body dns.Msg
}

// ReadResponse reads and returns HTTP response with encapsulated DNS
// response from r.
func ReadResponse(r *bufio.Reader) (*Response, error) {
	httpresp, err := http.ReadResponse(r, nil)
	if err != nil {
		return nil, err
	}

	defer httpresp.Body.Close()

	// ContentLength is unknown, response can't be extracted.
	bodyLen := httpresp.ContentLength
	if bodyLen < 0 {
		return nil, errContentLength
	}

	b, err := ioutil.ReadAll(io.LimitReader(httpresp.Body, bodyLen))
	if err != nil {
		return nil, err
	}

	var dnsresp dns.Msg
	err = dnsresp.Unpack(b)
	if err != nil {
		return nil, err
	}

	return &Response{
		ID:         int64(dnsresp.Id),
		StatusCode: httpresp.StatusCode,
		Header:     httpresp.Header,
		Body:       dnsresp,
	}, nil
}

type Handler interface {
	Handle(context.Context, *Request) (*Response, error)
}

type HandleServer interface {
	Handler
	Serve(context.Context) error
	Close() error
}

type Server struct {
	handlers []HandleServer
	logger   *zerolog.Logger
}

func NewServer(config *ServerConfig) (*Server, error) {
	srv := Server{
		handlers: make([]HandleServer, len(config.Resolvers)),
	}

	for i, r := range config.Resolvers {
		r := r
		newConn := func() *Conn {
			return &Conn{
				Root:            config.Root,
				Procname:        r.Name,
				Procenv:         r.Options,
				MaxIdleRequests: r.MaxIdle,
			}
		}

		// Ensure that one process starts when the configured number
		// of processes is not defined. (it's either 0 or not set).
		poolCap := r.Processes
		if poolCap < 1 {
			poolCap = 1
		}

		srv.handlers[i] = &ConnPool{Cap: poolCap, New: newConn}
	}

	return &srv, nil
}

func (srv *Server) Shutdown() error {
	for _, h := range srv.handlers {
		h.Close()
	}
	return nil
}

func (srv *Server) Serve(ctx context.Context) error {
	srv.logger = zerolog.Ctx(ctx)

	// TODO: reap processes after failure.
	for _, h := range srv.handlers {
		if err := h.Serve(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (srv *Server) ServeDNS(w dns.ResponseWriter, dnsreq *dns.Msg) {
	req := NewRequest(*dnsreq)

	var servfail dns.Msg
	servfail.SetRcode(dnsreq, dns.RcodeServerFailure)

	log := srv.logger.With().Uint16("id", dnsreq.Id).Logger()
	log.Debug().Msg("server received request")

	// Bypass the logger though context of the handler.
	ctx := log.WithContext(context.TODO())

	for no, h := range srv.handlers {
		log.Debug().Msgf("server passed request to %d resolver", no)
		resp, err := h.Handle(ctx, &req)

		if err != nil {
			log.Debug().Msgf("server error from %d resolver, %s", no, err)
			w.WriteMsg(&servfail)
			return
		}

		log.Debug().
			Int("status", resp.StatusCode).
			Int("rcode", resp.Body.Rcode).
			Msgf("server received reply")

		if resp.StatusCode == http.StatusOK {
			log.Debug().Msgf("server sending reply")
			w.WriteMsg(&resp.Body)
			return
		}
	}

	w.WriteMsg(&servfail)
}
