package ns

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequest_Forward(t *testing.T) {
	tests := []struct {
		laddr  net.Addr
		raddr  net.Addr
		header string
	}{
		{nil, nil, `by="unknown";for="unknown"`},
		{nil, anyAddr("2"), `by="unknown";for="2"`},
		{anyAddr("1"), nil, `by="1";for="unknown"`},
		{anyAddr("1"), anyAddr("2"), `by="1";for="2"`},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			req := new(Request).Forward(tt.laddr, tt.raddr)

			require.NotNil(t, req.Header)
			assert.Equal(t, tt.header, req.Header.Get("Forwarded"))
		})
	}
}

func TestTimeoutHandler(t *testing.T) {
	var resp Response

	fn := func(ctx context.Context, req *Request) (*Response, error) {
		_, ok := ctx.Deadline()
		assert.True(t, ok)
		return &resp, nil
	}

	h := TimeoutHandler(HandlerFunc(fn), time.Second)
	r, err := h.Handle(context.TODO(), nil)

	require.NoError(t, err)
	assert.Equal(t, resp, *r)
}

func newHandler(id string) HandlerFunc {
	return func(ctx context.Context, req *Request) (*Response, error) {
		var (
			header = make(http.Header)
			sc     = http.StatusTemporaryRedirect
		)
		if id != "" {
			header.Add("handler", id)
			sc = http.StatusOK
		}
		return &Response{Header: header, StatusCode: sc}, nil
	}
}

func TestMultiHandler_OK(t *testing.T) {
	tests := []struct {
		size int
		pos  int
	}{
		{1, 0},
		{2, 1},
		{3, 1},
		{3, 0},
		{5, 3},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			hh := make([]Handler, tt.size)

			for i := 0; i < tt.size; i++ {
				var id string
				if i == tt.pos {
					id = strconv.Itoa(i)
				}
				hh[i] = newHandler(id)
			}

			h := MultiHandler(hh...)
			resp, err := h.Handle(context.TODO(), new(Request))

			require.NoError(t, err)
			assert.Equal(t, strconv.Itoa(tt.pos), resp.Header.Get("handler"))
		})
	}
}

func TestMultiHandler_NoHandlers(t *testing.T) {
	h := MultiHandler(newHandler(""), newHandler(""))
	_, err := h.Handle(context.TODO(), new(Request))
	assert.Error(t, err)
}

func TestMultiHandler_Error(t *testing.T) {
	fn := func(ctx context.Context, req *Request) (*Response, error) {
		return nil, errors.New("failed")
	}

	h := MultiHandler(HandlerFunc(fn))
	_, err := h.Handle(context.TODO(), new(Request))
	assert.Error(t, err)
}
