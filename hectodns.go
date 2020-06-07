package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"

	"github.com/miekg/dns"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/netrack/hectodns/hectoserver"
)

type Proc struct {
	srvs []*hectoserver.Server
	lns  []*dns.Server
}

func NewProc(config *hectoserver.Config) (*Proc, error) {
	var (
		srvs []*hectoserver.Server
		lns  []*dns.Server
	)

	for _, sconf := range config.Servers {
		srv, err := hectoserver.NewServer(&sconf)
		if err != nil {
			return nil, err
		}

		drv := &dns.Server{
			Addr: sconf.Listen, Net: sconf.Proto, Handler: srv,
		}

		srvs = append(srvs, srv)
		lns = append(lns, drv)
	}

	return &Proc{srvs: srvs, lns: lns}, nil
}

func (p *Proc) Spawn(ctx context.Context) (err error) {
	log := zerolog.Ctx(ctx)

	for _, s := range p.srvs {
		if err = s.Serve(ctx); err != nil {
			return err
		}
	}

	// All listeners are blocking, therefore, spawn them in separate
	// routines, an wait until completion of all listeners.
	var (
		wg   sync.WaitGroup
		errs = make([]error, len(p.lns))
	)

	for i, ln := range p.lns {
		wg.Add(1)
		go func(i int, ln *dns.Server) {
			log.Info().Msgf("started %q at %s", ln.Net, ln.Addr)
			errs[i] = ln.ListenAndServe()
			wg.Done()
		}(i, ln)
	}

	wg.Wait()

	// Leave only the first non-nil error.
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Proc) Terminate() error {
	for _, s := range p.srvs {
		s.Shutdown()
	}
	// There is a bug in implementation of the DNS server, which prevents
	// it from being terminated from the concurrent goroutine, therefore
	// close all listeners at the end.
	for _, ln := range p.lns {
		ln.Shutdown()
	}
	return nil
}

func main() {
	// Configure console logger for the program.
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	ctx := log.Logger.WithContext(context.Background())

	var opts struct {
		configFile string
	}

	run := func(cmd *cobra.Command, args []string) {
		var proc *Proc

		termC := make(chan os.Signal, 1)
		signal.Notify(termC, os.Interrupt, os.Kill)

		go func() {
			http.ListenAndServe("localhost:6060", nil)
		}()
		go func() {
			if <-termC; proc != nil {
				proc.Terminate()
			}
		}()

		config, err := hectoserver.DecodeConfig(opts.configFile)
		if err != nil {
			log.Fatal().Msg(err.Error())
		}

		proc, err = NewProc(config)
		if err != nil {
			log.Fatal().Msg(err.Error())
		}

		if err = proc.Spawn(ctx); err != nil {
			log.Fatal().Msg(err.Error())
		}
	}

	cmd := cobra.Command{
		Use:   "hectodns",
		Short: "hectodns - a command to launch Hecto DNS server",
		Run:   run,
	}

	cmd.Flags().StringVarP(&opts.configFile, "config-file", "c", "", "hectodns-specific configuration file")
	cmd.MarkFlagRequired("config-file")

	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
	}
}
