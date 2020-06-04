package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"

	"github.com/miekg/dns"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/netrack/hectodns/hectoserver"
)

func main() {
	var (
		srv *hectoserver.Server
		drv *dns.Server
	)

	termC := make(chan os.Signal, 1)
	signal.Notify(termC, os.Interrupt)

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
	go func() {
		<-termC
		if srv != nil {
			srv.Shutdown()
		}
		if drv != nil {
			drv.Shutdown()
		}
	}()

	config, err := hectoserver.DecodeConfig("hectodns.conf")
	if err != nil {
		panic(err)
	}

	fmt.Printf("%#v\n", config)

	srv, err = hectoserver.NewServer(config.Servers[0].Root, config.Servers[0].Resolvers)
	if err != nil {
		panic(err)
	}

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	ctx := log.Logger.WithContext(context.Background())

	if err := srv.Serve(ctx); err != nil {
		panic(err)
	}

	drv = &dns.Server{Addr: ":5333", Net: "udp", Handler: srv}
	if err = drv.ListenAndServe(); err != nil {
		panic(err)
	}
}
