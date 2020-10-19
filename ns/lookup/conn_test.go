package lookup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConn_Close(t *testing.T) {
	var conn Conn

	err := conn.Close(context.TODO())
	require.NoError(t, err)

	// Ensure that new connection cannot be closed.
	assert.Equal(t, StateNew, conn.state, "new connection cannot be closed")
}
