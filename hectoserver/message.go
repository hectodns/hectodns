package hectoserver

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"

	"github.com/miekg/dns"
)

// Message is the wrapper around the DNS message that puts length of
// the DNS request (or response) before the body.
type Message struct {
	Body dns.Msg
}

// WriteTo write the message to the given writer in the binary format.
func (m *Message) WriteTo(w io.Writer) (n int64, err error) {
	buf, err := m.Body.Pack()
	if err != nil {
		return
	}

	return writeTo(w, uint16(len(buf)), buf)
}

// ReadFrom reads a message from the given read in the binary format.
func (m *Message) ReadFrom(r io.Reader) (n int64, err error) {
	var nn int64
	var length uint16

	n, err = readFrom(r, &length)
	if err != nil {
		return
	}

	limrd := io.LimitReader(r, int64(length))

	buf, err := ioutil.ReadAll(limrd)
	if n += nn; err != nil {
		return
	}

	err = m.Body.Unpack(buf)
	return
}

func writeTo(w io.Writer, v ...interface{}) (int64, error) {
	var (
		wbuf bytes.Buffer
		err  error
	)

	for _, elem := range v {
		switch elem := elem.(type) {
		case nil:
			continue
		case io.WriterTo:
			_, err = elem.WriteTo(&wbuf)
		default:
			err = binary.Write(&wbuf, binary.BigEndian, elem)
		}

		if err != nil {
			return 0, err
		}
	}

	return wbuf.WriteTo(w)
}

// reader type used to calculate the count of bytes retrieved from the
// configured reader instance.
type reader struct {
	io.Reader
	read int64
}

// Read implements io.Reader interface.
func (r *reader) Read(b []byte) (int, error) {
	n, err := r.Reader.Read(b)
	r.read += int64(n)
	return n, err
}

func readFrom(r io.Reader, v ...interface{}) (int64, error) {
	var (
		num int64
		err error
	)

	rd := &reader{r, 0}

	for _, elem := range v {
		switch elem := elem.(type) {
		case io.ReaderFrom:
			num, err = elem.ReadFrom(r)
			rd.read += num
		default:
			err = binary.Read(rd, binary.BigEndian, elem)
		}

		if err != nil {
			return rd.read, err
		}
	}

	return rd.read, nil
}
