package ns

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/hectodns/hectodns/internal/errutil"

	"github.com/miekg/dns"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/netutil"
)

const (
	defaultAddr  = ":53"
	defaultProto = "udp"
)

var (
	ErrProtoUnknown = Error{err: "unknown protocol"}
)

type Shutdowner interface {
	Shutdown(context.Context) error
}

// ShutdownFunc tells a handler to terminate its work. ShutdownFunc
// waits for the work to stop. A ShutdownFunc may be called by multiple
// goroutines simultaneously.
type ShutdownFunc func(context.Context) error

func (fn ShutdownFunc) Shutdown(ctx context.Context) error {
	return fn(ctx)
}

// ShutdownAll executes all passed ShutdownFunc concurrently.
func ShutdownAll(ctx context.Context, ss ...Shutdowner) (err error) {
	errC := make(chan error, len(ss))

	for _, s := range ss {
		go func(s Shutdowner) {
			if s != nil {
				errC <- s.Shutdown(ctx)
			} else {
				errC <- nil
			}
		}(s)
	}

	for i := 0; i < len(ss); i++ {
		err = errutil.Join(err, <-errC)
	}
	return
}

func MultiShutdown(ss ...Shutdowner) ShutdownFunc {
	return func(ctx context.Context) error {
		return ShutdownAll(ctx, ss...)
	}
}

// Server describes an interface to start and terminate a server.
type Listener interface {
	ListenAndServe() error
	Shutdowner
}

// loggingListener is a listener that puts address information into the log.
type loggingListener struct {
	Listener

	proto string
	addr  string
}

func (ll loggingListener) ListenAndServe() error {
	log.Info().Msgf("started (%s) at %s", ll.proto, ll.addr)
	return ll.Listener.ListenAndServe()
}

type ListenConfig struct {
	Addr           string
	MaxConns       int
	RequestTimeout time.Duration
}

func Listen(proto string, lc ListenConfig, h Handler) (Listener, error) {
	if lc.Addr == "" {
		lc.Addr = defaultAddr
	}
	if proto == "" {
		proto = defaultProto
	}

	var ln Listener

	if lc.RequestTimeout != 0 {
		// Add a timeout for processing each request.
		h = TimeoutHandler(h, lc.RequestTimeout)
		log.Debug().Msgf("set %s request timeout", lc.RequestTimeout)
	}

	switch proto {
	case "udp":
		ln = ListenUDP(lc, h)
	case "tcp":
		ln = ListenTCP(lc, h)
	case "http":
		ln = ListenHTTP(lc, h)
	default:
		return nil, ErrProtoUnknown
	}

	return loggingListener{Listener: ln, proto: proto, addr: lc.Addr}, nil
}

type httpServer struct {
	http.Server
	ListenConfig
}

func (s *httpServer) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.ListenConfig.Addr)
	if err != nil {
		return err
	}

	if s.ListenConfig.MaxConns > 0 {
		ln = netutil.LimitListener(ln, s.ListenConfig.MaxConns)
		log.Debug().Msgf("set max connections to %d", s.ListenConfig.MaxConns)
	}

	return s.Server.Serve(ln)
}

func ListenHTTP(lc ListenConfig, h Handler) Listener {
	return &httpServer{
		Server:       http.Server{Handler: ServeHTTP(h)},
		ListenConfig: lc,
	}
}

func ServeHTTP(h Handler) http.Handler {
	return &httpHandler{h}
}

type httpHandler struct {
	Handler
}

func (h *httpHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet, http.MethodPost:
	default:
		http.Error(rw, "", http.StatusMethodNotAllowed)
	}

	fwreq, err := ParseRequest(req)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := log.Logger.WithContext(req.Context())

	resp, err := h.Handle(ctx, fwreq)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	buf, _ := resp.Body.Pack()

	rw.Header().Set("Content-Type", "application/dns-message")
	rw.Header().Set("Content-Length", strconv.Itoa(len(buf)))

	rw.WriteHeader(http.StatusOK)
	rw.Write(buf)
}

func ListenUDP(lc ListenConfig, h Handler) Listener {
	return &dnsServer{
		Server: dns.Server{Addr: lc.Addr, Net: "udp", Handler: ServeDNS(h)},
	}
}

func ListenTCP(lc ListenConfig, h Handler) Listener {
	return &dnsServer{
		Server: dns.Server{Addr: lc.Addr, Net: "tcp", Handler: ServeDNS(h)},
	}
}

func ServeDNS(h Handler) dns.Handler {
	return &dnsHandler{h}
}

type dnsServer struct {
	dns.Server
}

func (s *dnsServer) Shutdown(ctx context.Context) error {
	return s.Server.ShutdownContext(ctx)
}

type dnsHandler struct {
	Handler
}

func (h *dnsHandler) ServeDNS(rw dns.ResponseWriter, r *dns.Msg) {
	var servfail dns.Msg
	servfail.SetRcode(r, dns.RcodeServerFailure)

	ctx := log.Logger.WithContext(context.Background())

	fwreq := NewRequest(*r).Forward(rw.LocalAddr(), rw.RemoteAddr())
	resp, err := h.Handle(ctx, fwreq)
	if err == nil {
		rw.WriteMsg(&resp.Body)
		return
	}

	rw.WriteMsg(&servfail)
}
