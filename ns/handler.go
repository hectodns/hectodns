package ns

import (
	"context"
	"encoding/base64"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

type Handler interface {
	Handle(context.Context, *Request) (*Response, error)
}

// HandlerFunc is a function adapter to allow use of ordinary functions
// as request handlers. If f is a function with the appropriate signature,
// HandlerFunc(f) is a Handler that calls f.
type HandlerFunc func(context.Context, *Request) (*Response, error)

// Handle implements Handler interface, calls f(ctx, req)
func (fn HandlerFunc) Handle(ctx context.Context, req *Request) (*Response, error) {
	return fn(ctx, req)
}

// TimeoutHandler is a handler that limits the handler execution by the
// specified timeout. Wrapped handler must implement context cancellation.
//
// It adds a timeout to the passed context and then executes a wrapped
// handler instance.
func TimeoutHandler(h Handler, t time.Duration) HandlerFunc {
	return func(ctx context.Context, req *Request) (*Response, error) {
		ctx, cancel := context.WithTimeout(ctx, t)
		defer cancel()
		return h.Handle(ctx, req)
	}
}

// MultiHandler creates a handler that executes all passed handlers one
// by one until one of them successfully processes the request.
//
// If a listed handler returns an error, the overall handle operation
// stops and returns the error; it does not continue down the list.
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

	if log.GetLevel() <= zerolog.DebugLevel {
		b, _ := req.Body.Pack()
		b64 := base64.RawURLEncoding.EncodeToString(b)
		log.Debug().Str("b64", b64).Msg("request received")
	}

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
