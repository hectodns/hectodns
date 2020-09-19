package hectoserver

import (
	"net"
	"testing"

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
