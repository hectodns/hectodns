package lookup

import (
	"bufio"
	"io"
	"sync"
)

type chanResponse struct {
	ret interface{}
	err error
}

// The chanReader is a wrapper around connection reader used to
// create a channel of Responses by continuosly fetching responses
// from the connection.
type chanReader struct {
	// Read is the function used to read the data from the
	// buffered reader.
	Read func(*bufio.Reader) chanResponse

	once sync.Once
	ch   chan chanResponse
}

// read starts and inifinite loop of reading the responses from
// the connection. When the read call returns an error, a submission
// channel closes and loop terminates.
func (r *chanReader) read(rd io.Reader) {
	bufr := bufio.NewReader(rd)

	for {
		resp := r.Read(bufr)
		r.ch <- resp

		if resp.err != nil {
			close(r.ch)
			return
		}
	}
}

// The start creates a receive channel to deliver responses to clients,
// spawns a new goroutine to read responses from the reader and send
// them through the channel.
func (r *chanReader) start(rd io.Reader) {
	r.ch = make(chan chanResponse, 1)
	go r.read(rd)
}

// C returns a read-only channel of response plus error.
func (r *chanReader) C(rd io.Reader) <-chan chanResponse {
	r.once.Do(func() { r.start(rd) })
	return r.ch
}
