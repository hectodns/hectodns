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
	"github.com/pkg/errors"
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

	Header http.Header

	Body dns.Msg

	// At is a request submission timestamp.
	At time.Time
}

func NewRequest(dnsreq dns.Msg) *Request {
	return &Request{
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

func MultiHandler(hh ...Handler) Handler {
	return &multiHandler{hh}
}

type multiHandler struct {
	handlers []Handler
}

// Handle implements Handler interface. Method sequentially executes
// handlers until the successful response is received.
//
// Headers from previous response are copied to the next request, so
// the handlers could influence behaviour of next handlers.
func (mh *multiHandler) Handle(ctx context.Context, req *Request) (*Response, error) {
	log := zerolog.Ctx(ctx).With().Uint16("id", req.Body.Id).Logger()
	log.Debug().Msg("received request")

	// Bypass the logger though context of the handler.
	ctx = log.WithContext(ctx)

	for no, h := range mh.handlers {
		log.Debug().Msgf("passed request to %d resolver", no)
		resp, err := h.Handle(ctx, req)

		if err != nil {
			log.Debug().Msgf("error from %d resolver, %s", no, err)
			return nil, err
		}

		log.Debug().
			Int("status", resp.StatusCode).
			Int("rcode", resp.Body.Rcode).
			Msgf("received reply")

		if resp.StatusCode == http.StatusOK {
			log.Debug().Msgf("sending reply")
			return resp, nil
		}

		// Bypass headers to the next handlers, so the handlers could
		// exchange intermediate information.
		req.Header = resp.Header
	}

	return nil, errors.New("no handlers left")
}
