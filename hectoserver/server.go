package hectoserver

import (
	"context"
	"encoding/base64"
	"net"
	"net/http"
	"strconv"

	"github.com/miekg/dns"
	"github.com/rs/zerolog/log"
)

var (
	ErrProtoUnknown = Error{err: "unknown protocol"}
)

// Server describes an interface to start and terminate a server.
type Listener interface {
	ListenAndServe() error
	Shutdown(context.Context) error
}

func Listen(proto, addr string, h Handler) (Listener, error) {
	switch proto {
	case "udp":
		return ListenUDP(addr, h), nil
	case "tcp":
		return ListenTCP(addr, h), nil
	case "http":
		return ListenHTTP(addr, h), nil
	default:
		return nil, ErrProtoUnknown
	}
}

func ListenHTTP(addr string, h Handler) Listener {
	return &http.Server{Addr: addr, Handler: ServeHTTP(h)}
}

func ServeHTTP(h Handler) http.Handler {
	return &httpHandler{h}
}

type httpHandler struct {
	Handler
}

type anyAddr string

func (a anyAddr) Network() string { return "any" }
func (a anyAddr) String() string  { return string(a) }

func (h *httpHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	ctx := log.Logger.WithContext(req.Context())
	laddr, _ := req.Context().Value(http.LocalAddrContextKey).(net.Addr)

	switch req.Method {
	case http.MethodGet:
		query := req.URL.Query().Get("dns")
		if query == "" {
			rw.WriteHeader(http.StatusBadRequest)
			http.Error(rw, "missing 'dns' query in request", http.StatusBadRequest)
			return
		}

		b64, err := base64.RawURLEncoding.DecodeString(query)
		if err != nil {
			http.Error(rw, "", http.StatusBadRequest)
			return
		}

		var r dns.Msg
		err = r.Unpack(b64)
		if err != nil {
			http.Error(rw, "broken 'dns' query", http.StatusBadRequest)
			return
		}

		fwreq := NewRequest(r).Forward(laddr, anyAddr(req.RemoteAddr))
		fwreq.RequestURI = req.RequestURI

		resp, err := h.Handle(ctx, fwreq)
		if err != nil {
			http.Error(rw, "no response", http.StatusInternalServerError)
			return
		}

		buf, _ := resp.Body.Pack()

		rw.Header().Set("Content-Type", "application/dns-message")
		rw.Header().Set("Content-Length", strconv.Itoa(len(buf)))

		rw.WriteHeader(http.StatusOK)
		rw.Write(buf)
	default:
		http.Error(rw, "", http.StatusMethodNotAllowed)
	}
}

func ListenUDP(addr string, h Handler) Listener {
	return &dnsServer{
		Server: dns.Server{Addr: addr, Net: "udp", Handler: ServeDNS(h)},
	}
}

func ListenTCP(addr string, h Handler) Listener {
	return &dnsServer{
		Server: dns.Server{Addr: addr, Net: "tcp", Handler: ServeDNS(h)},
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
