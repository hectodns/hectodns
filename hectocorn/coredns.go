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

func (r *response) LocalAddr() net.Addr         { return r.laddr }
func (r *response) RemoteAddr() net.Addr        { return r.raddr }
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

	Log.Debug("starting...")

	for {
		req, err := hectoserver.ReadRequest(buf.Reader)
		if err != nil {
			Log.Error(err.Error())
			return
		}

		fwh, err := hectoserver.ParseForwarded(req.Header.Get("Forwarded"))
		if err != nil {
			Log.Error(err.Error())
			return
		}

		if len(fwh.For) == 0 {
			continue
		}

		rw := response{laddr: &fwh.By, raddr: &fwh.For[len(fwh.For)-1]}

		rcode, err := h.ServeDNS(context.TODO(), &rw, &req.Body)
		if err != nil {
			Log.Error(err.Error())
			return
		}

		if rcode != dns.RcodeSuccess {
			Log.Error("failed to process request")
			return
		}

		resp := hectoserver.NewResponse(*rw.msg)

		err = resp.Write(buf)
		if err != nil {
			Log.Error("failed to write response")
		}

		buf.Flush()
	}
}
