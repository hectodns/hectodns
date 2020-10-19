package ns

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/miekg/dns"
)

const (
	httpMethod    = http.MethodPost
	httpUserAgent = "HectoDNS"
)

// Error represents a server error.
type Error struct{ err string }

func NewError(text string) Error {
	return Error{err: text}
}

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

func readDNS(r io.Reader, rlen int64) (m *dns.Msg, err error) {
	// ContentLength is unknown, DNS message can't be extracted.
	if rlen < 0 {
		return nil, errContentLength
	}

	b, err := ioutil.ReadAll(io.LimitReader(r, rlen))
	if err != nil {
		return nil, err
	}

	m = new(dns.Msg)
	if err = m.Unpack(b); err != nil {
		return nil, err
	}
	return m, nil
}

type Request struct {
	// ID is a unique identifier of the request. It's the same as DNS
	// request identifier.
	ID int64

	// RequestURI is a path where request was submitted.
	//
	// For some applications it is usefull to extract parameters from
	// the URL. This URL will be copied to each plugin.
	RequestURI string

	Header http.Header

	Body dns.Msg

	// At is a request submission timestamp.
	At time.Time
}

func NewRequest(dnsreq dns.Msg) *Request {
	return &Request{
		ID:     int64(dnsreq.Id),
		Header: make(http.Header),
		Body:   dnsreq,
		At:     time.Now(),
	}
}

type anyAddr string

func (a anyAddr) Network() string { return "any" }
func (a anyAddr) String() string  { return string(a) }

func ParseRequest(req *http.Request) (*Request, error) {
	var (
		bytes []byte
		err   error
	)

	switch req.Method {
	case http.MethodGet:
		query := req.URL.Query().Get("dns")
		if query == "" {
			return nil, Error{err: "missing 'dns' query in request"}
		}

		bytes, err = base64.RawURLEncoding.DecodeString(query)
		if err != nil {
			return nil, Error{err: err.Error()}
		}
	case http.MethodPost:
		limrd := io.LimitReader(req.Body, req.ContentLength)
		bytes, err = ioutil.ReadAll(limrd)
	default:
		return nil, Error{err: "not supported HTTP method"}
	}

	var r dns.Msg
	err = r.Unpack(bytes)
	if err != nil {
		return nil, Error{err: "broken 'dns' query"}
	}

	laddr, _ := req.Context().Value(http.LocalAddrContextKey).(net.Addr)

	fwreq := NewRequest(r).Forward(laddr, anyAddr(req.RemoteAddr))
	fwreq.RequestURI = req.RequestURI

	return fwreq, nil
}

// ReadRequest reads and parses an incoming request from r.
//
// ReadRequst is a low-level function and should only be used for specialized
// applications; most code should use Listener to read request and handle
// them via Handler interface.
func ReadRequest(r *bufio.Reader) (*Request, error) {
	httpreq, err := http.ReadRequest(r)
	if err != nil {
		return nil, err
	}

	defer httpreq.Body.Close()

	m, err := readDNS(httpreq.Body, httpreq.ContentLength)
	if err != nil {
		return nil, err
	}

	req := NewRequest(*m)
	req.Header = httpreq.Header

	return req, nil
}

// Forward sets the "Forwarded" header defined in RFC 7239, section 4. This
// is used to pass local and remote address to the processing plugins.
func (r *Request) Forward(laddr, raddr net.Addr) *Request {
	if r.Header == nil {
		r.Header = make(http.Header)
	}

	// Specify that forwarding entity is not known.
	if laddr == nil {
		laddr = anyAddr("unknown")
	}
	if raddr == nil {
		raddr = anyAddr("unknown")
	}
	r.Header.Set("Forwarded", fmt.Sprintf("by=%q;for=%q", laddr, raddr))
	return r
}

func (r *Request) Write(w io.Writer) error {
	b, err := r.Body.Pack()
	if err != nil {
		return err
	}

	httpURL := "/"
	if r.RequestURI != "" {
		httpURL = r.RequestURI
	}

	httpreq, err := http.NewRequest(httpMethod, httpURL, bytes.NewBuffer(b))
	if err != nil {
		return err
	}

	httpreq.Header.Set("User-Agent", httpUserAgent)

	// Override request headers with user-defined headers,
	// so the plugins could be access to this information.
	if header := r.Header; header != nil {
		for key, value := range header {
			httpreq.Header[key] = value
		}
	}

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

func NewResponse(m dns.Msg) *Response {
	return &Response{
		ID:         int64(m.Id),
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       m,
	}
}

// ReadResponse reads and returns HTTP response with encapsulated DNS
// response from r.
func ReadResponse(r *bufio.Reader) (*Response, error) {
	httpresp, err := http.ReadResponse(r, nil)
	if err != nil {
		return nil, err
	}

	defer httpresp.Body.Close()

	m, err := readDNS(httpresp.Body, httpresp.ContentLength)
	if err != nil {
		return nil, err
	}

	return &Response{
		ID:         int64(m.Id),
		StatusCode: httpresp.StatusCode,
		Header:     httpresp.Header,
		Body:       *m,
	}, nil
}

func (r *Response) Write(w io.Writer) error {
	b, err := r.Body.Pack()
	if err != nil {
		return err
	}

	httpresp := http.Response{
		StatusCode:    r.StatusCode,
		Header:        r.Header,
		Body:          ioutil.NopCloser(bytes.NewReader(b)),
		ContentLength: int64(len(b)),
	}

	return httpresp.Write(w)
}

func (r *Response) Redirect() *Response {
	r.StatusCode = http.StatusTemporaryRedirect
	return r
}

type ForwardedHeader struct {
	Proto string
	Host  string
	By    net.TCPAddr
	For   []net.TCPAddr
}

// ParseForwarded parses a Forwarded header and returns all passed
// directives within a forwarded map.
func ParseForwarded(header string) (fw ForwardedHeader, err error) {
	var (
		raw_dirs = strings.Split(header, ";")
		dirs     = make([]string, 0, len(raw_dirs))
	)

	// Each directive can represent a list of values, like it happens for
	// a directive "for":
	//
	// 	Forwarded: for=192.168.0.1,for=192.168.0.2;by=192.168.0.254
	for _, directive := range raw_dirs {
		dirs = append(dirs, strings.Split(directive, ",")...)
	}

	for _, directive := range dirs {
		// Each directive reprsents a key-value pair: by=1.2.3.4
		kv := strings.SplitN(strings.Trim(directive, " "), "=", 2)
		if len(kv) != 2 {
			return ForwardedHeader{}, err
		}

		key := strings.ToLower(kv[0])
		val := kv[1]

		// For directives that does not support lists (proto, host, by),
		// simply override the value when a new identifier appears.
		switch key {
		case "proto":
			fw.Proto = val
		case "host":
			fw.Host = val
		case "by":
			ip, port, err := parseHostPort(val)
			if err != nil {
				return ForwardedHeader{}, err
			}
			fw.By = net.TCPAddr{IP: ip, Port: port}
		case "for":
			ip, port, err := parseHostPort(val)
			if err != nil {
				return ForwardedHeader{}, err
			}
			fw.For = append(fw.For, net.TCPAddr{IP: ip, Port: port})
		}
	}

	return
}

func parseHostPort(s string) (ip net.IP, port int, err error) {
	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		s = strings.TrimLeft(s, `"`)
		s = strings.TrimRight(s, `"`)
	}

	portIndex := strings.LastIndex(s, ":")

	if portIndex > 0 {
		i, err := strconv.ParseInt(s[portIndex+1:], 10, 0)
		if err != nil {
			return nil, 0, err
		}

		port = int(i)
		s = s[:portIndex]
	}

	if strings.HasPrefix(s, `[`) && strings.HasSuffix(s, `]`) {
		s = strings.TrimLeft(s, `[`)
		s = strings.TrimRight(s, `]`)
	}

	ip = net.ParseIP(s)
	if ip == nil {
		return ip, port, &net.ParseError{Type: "IP address", Text: s}
	}

	return
}
