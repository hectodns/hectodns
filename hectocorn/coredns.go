package hectocorn

import (
	"bufio"
	"context"
	"io"
	"net"
	"os"

	"github.com/miekg/dns"

	"github.com/hectodns/hectodns/hectoserver"
)

// CoreHandler is an interface for CoreDNS plugins without the Name
// method. This extension allows to reuse CoreDNS plugins almost
// without modifications.
type CoreHandler interface {
	ServeDNS(context.Context, dns.ResponseWriter, *dns.Msg) (int, error)
}

type response struct {
	msg          *dns.Msg
	laddr, raddr net.Addr
}

func (r *response) LocalAddr() net.Addr { panic("local addr") }

func (r *response) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5335}
}

func (r *response) WriteMsg(msg *dns.Msg) error { r.msg = msg; return nil }
func (r *response) Write([]byte) (int, error)   { return 0, io.EOF }
func (r *response) Close() error                { return nil }
func (r *response) TsigStatus() error           { return nil }
func (r *response) TsigTimersOnly(bool)         { panic("tsig timers only") }
func (r *response) Hijack()                     { panic("hijack") }

func ServeCore(h CoreHandler) {
	r := bufio.NewReader(os.Stdin)
	w := bufio.NewWriter(os.Stdout)

	buf := bufio.NewReadWriter(r, w)

	os.Stderr.WriteString("d:starting ...\n")

	for {
		req, err := hectoserver.ReadRequest(buf.Reader)
		if err != nil {
			os.Stderr.WriteString("e:" + err.Error() + "\n")
			return
		}

		var rw response
		rcode, err := h.ServeDNS(context.TODO(), &rw, &req.Body)
		if err != nil {
			os.Stderr.WriteString("e:" + err.Error() + "\n")
			return
		}

		if rcode != dns.RcodeSuccess {
			os.Stderr.WriteString("e:failed to process\n")
			return
		}

		resp := hectoserver.NewResponse(*rw.msg)

		err = resp.Write(buf)
		if err != nil {
			os.Stderr.WriteString("e:failed to write response\n")
		}

		buf.Flush()
	}
}
