package main

import (
	"encoding/binary"
	"os"

	"github.com/miekg/dns"
)

func main() {
	for {
		var length uint16
		if err := binary.Read(os.Stdin, binary.BigEndian, &length); err != nil {
			panic(err)
		}

		buf := make([]byte, length)
		_, err := os.Stdin.Read(buf)
		if err != nil {
			panic(err)
		}

		var req dns.Msg
		err = req.Unpack(buf)
		if err != nil {
			panic(err)
		}

		var resp dns.Msg
		resp.SetRcode(&req, dns.RcodeRefused)
		b, err := resp.Pack()
		if err != nil {
			panic(err)
		}

		if err := binary.Write(os.Stdout, binary.BigEndian, uint16(len(b))); err != nil {
			panic(err)
		}
		os.Stdout.Write(b)
	}
}
