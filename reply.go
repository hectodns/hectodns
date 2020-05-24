package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/miekg/dns"
)

func main() {
	r := bufio.NewReader(os.Stdin)
	w := bufio.NewWriter(os.Stdout)
	buf := bufio.NewReadWriter(r, w)

	for {
		httpreq, err := http.ReadRequest(buf.Reader)
		os.Stderr.Write([]byte(fmt.Sprintf("%#v\n", httpreq)))
		if err != nil {
			panic(err)
		}

		b, err := ioutil.ReadAll(io.LimitReader(httpreq.Body, httpreq.ContentLength))
		if err != nil {
			panic(err)
		}

		httpreq.Body.Close()
		os.Stderr.Write([]byte("closed\n"))

		var dnsreq dns.Msg
		err = dnsreq.Unpack(b)
		if err != nil {
			panic(err)
		}

		os.Stderr.Write([]byte(fmt.Sprintf("%#v\n", dnsreq)))

		var dnsresp dns.Msg
		dnsresp.SetRcode(&dnsreq, dns.RcodeRefused)
		b, err = dnsresp.Pack()
		if err != nil {
			panic(err)
		}

		httpresp := &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: int64(len(b)),
			Body:          ioutil.NopCloser(bytes.NewBuffer(b)),
		}

		httpresp.Write(buf)
		buf.Flush()
	}
}
