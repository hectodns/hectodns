package main

import (
	"context"
	"fmt"

	"github.com/miekg/dns"
	"github.com/netrack/hectodns/hectoserver"
)

func main() {
	config, err := hectoserver.DecodeConfig("hectodns.conf")
	if err != nil {
		panic(err)
	}

	fmt.Printf("%#v\n", config)

	srv := hectoserver.NewServer(config.Servers[0].Resolvers)
	if err := srv.Serve(context.Background()); err != nil {
		panic(err)
	}

	err = dns.ListenAndServe(":5333", "udp", srv)
	if err != nil {
		panic(err)
	}
}
