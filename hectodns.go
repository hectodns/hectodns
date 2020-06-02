package main

import (
	"context"
	"fmt"
	"os"

	"github.com/miekg/dns"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/netrack/hectodns/hectoserver"
)

func main() {
	config, err := hectoserver.DecodeConfig("hectodns.conf")
	if err != nil {
		panic(err)
	}

	fmt.Printf("%#v\n", config)

	srv, err := hectoserver.NewServer(config.Servers[0].Root, config.Servers[0].Resolvers)
	if err != nil {
		panic(err)
	}

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	ctx := log.Logger.WithContext(context.Background())

	if err := srv.Serve(ctx); err != nil {
		panic(err)
	}

	err = dns.ListenAndServe(":5333", "udp", srv)
	if err != nil {
		panic(err)
	}
}
