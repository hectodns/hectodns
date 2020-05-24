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
	httpMethod = http.MethodPost
	httpURL    = "/dns-query"
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

type Request struct {
	// ID is a unique identifier of the request.
	ID int64

	// Body is the network-encoded representation of the request.
	//
	// Depending on the protocol for communication with a resolver,
	// it could be an text-encoded HTTP request or binary-encoded DNS
	// query.
	Body io.Reader

	// At is a request submission timestamp.
	At time.Time
}

func NewRequest(dnsreq *dns.Msg) (*Request, error) {
	b, err := dnsreq.Pack()
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequest(httpMethod, httpURL, bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}

	var body bytes.Buffer
	if err = httpReq.Write(&body); err != nil {
		return nil, err
	}

	return &Request{
		ID:   int64(dnsreq.Id),
		Body: &body,
		At:   time.Now(),
	}, nil
}

// Bytes returns bytes representation of the request body.
func (r *Request) Bytes() ([]byte, error) {
	return ioutil.ReadAll(r.Body)
}

type Response struct {
	ID int64

	StatusCode int

	Header http.Header

	Body io.Reader
}

func (resp *Response) UnmarshalDNS() (*dns.Msg, error) {
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var msg dns.Msg
	err = msg.Unpack(b)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

// ResponseReader is used by a Conn to retrieve a response from the stdout.
//
// This inferface is used to support multiple communication protocols with
// processes responsible for resolving the DNS requests.
type ResponseReader interface {
	Read(r *bufio.Reader) (*Response, error)
}

type httpRR struct {
}

func (rr httpRR) Read(r *bufio.Reader) (*Response, error) {
	httpresp, err := http.ReadResponse(r, nil)
	if err != nil {
		return nil, err
	}

	log.Printf("!!! %#v\n", httpresp)

	defer httpresp.Body.Close()

	b, err := ioutil.ReadAll(io.LimitReader(httpresp.Body, httpresp.ContentLength))
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
		Body:       bytes.NewBuffer(b),
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

func NewServer(cc []ResolverConfig) *Server {
	srv := Server{
		handlers: make([]HandleServer, len(cc)),
	}
	for i, c := range cc {
		srv.handlers[i] = &Conn{
			Procname:       c.Name,
			ResponseReader: httpRR{},
		}
	}
	return &srv
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
	req, err := NewRequest(dnsreq)
	if err != nil {
		panic(err)
	}

	var servfail dns.Msg
	servfail.SetRcode(dnsreq, dns.RcodeServerFailure)

	for i, h := range srv.handlers {
		log.Println(i)
		resp, err := h.Handle(context.TODO(), req)
		log.Printf("INFO: %d processed %#v", i, resp)

		if err != nil {
			log.Println("FATAL:", err.Error())
			w.WriteMsg(&servfail)
			return
		}

		if resp.StatusCode == http.StatusOK {
			dnsresp, err := resp.UnmarshalDNS()
			if err != nil {
				log.Println("FATAL: 2", err.Error())
				w.WriteMsg(&servfail)
				return
			}
			w.WriteMsg(dnsresp)
			return
		}
	}

	w.WriteMsg(&servfail)
}
