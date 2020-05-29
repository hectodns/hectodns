package hectoserver

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/miekg/dns"
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

type ResponseWriter interface {
	Write(resp *Response) error
}

type Handler interface {
	Handle(context.Context, *Request) (*Response, error)
}

type HandleServer interface {
	Handler
	Serve(context.Context) error
}

type Server struct {
	handlers []HandleServer
}

func NewServer(cc []ResolverConfig) (*Server, error) {
	srv := Server{
		handlers: make([]HandleServer, len(cc)),
	}
	for i, c := range cc {
		c := c
		newConn := func() *Conn {
			return &Conn{
				Procname:        c.Name,
				Procopts:        c.Options,
				MaxIdleRequests: c.MaxIdle,
			}
		}

		// Ensure that one process starts when the configured number
		// of processes is not defined. (it's either 0 or not set).
		poolCap := c.Processes
		if poolCap < 1 {
			poolCap = 1
		}

		srv.handlers[i] = &ConnPool{Cap: poolCap, New: newConn}
	}
	return &srv, nil
}

func (srv *Server) Serve(ctx context.Context) error {
	// TODO: reap processes after failure.
	for _, r := range srv.handlers {
		if err := r.Serve(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (srv *Server) ServeDNS(w dns.ResponseWriter, dnsreq *dns.Msg) {
	req := NewRequest(*dnsreq)

	var servfail dns.Msg
	servfail.SetRcode(dnsreq, dns.RcodeServerFailure)

	for i, h := range srv.handlers {
		log.Println(i)
		resp, err := h.Handle(context.TODO(), &req)
		log.Printf("INFO: %d processed %#v", i, resp)

		if err != nil {
			log.Println("FATAL:", err.Error())
			w.WriteMsg(&servfail)
			return
		}

		if resp.StatusCode == http.StatusOK {
			w.WriteMsg(&resp.Body)
			return
		}
	}

	w.WriteMsg(&servfail)
}
