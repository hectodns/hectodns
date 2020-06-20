package hectoserver

import (
	"context"
	"encoding/base64"
	"net/http"
	"strconv"

	"github.com/miekg/dns"
	"github.com/rs/zerolog/log"
)

func ServeHTTP(h Handler) http.Handler {
	return &httpHandler{h}
}

type httpHandler struct {
	Handler
}

func (h *httpHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	ctx := log.Logger.WithContext(req.Context())

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

		resp, err := h.Handle(ctx, NewRequest(r))
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

func ServeDNS(h Handler) dns.Handler {
	return &dnsHandler{h}
}

type dnsHandler struct {
	Handler
}

func (h *dnsHandler) ServeDNS(rw dns.ResponseWriter, r *dns.Msg) {
	var servfail dns.Msg
	servfail.SetRcode(r, dns.RcodeServerFailure)

	ctx := log.Logger.WithContext(context.Background())

	resp, err := h.Handle(ctx, NewRequest(*r))
	if err == nil {
		rw.WriteMsg(&resp.Body)
		return
	}

	rw.WriteMsg(&servfail)
}
