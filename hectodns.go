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

	"github.com/hectodns/hectodns/hectoserver"
)

type Proc struct {
	ctx      context.Context
	shutdown hectoserver.ShutdownFunc

	dnsListeners  []*dns.Server
	httpListeners []*http.Server
}

func NewProc(config *hectoserver.Config) (*Proc, error) {
	var shutdowners []hectoserver.ShutdownFunc

	// Configure console logger for the program.
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	ctx := log.Logger.WithContext(context.Background())

	proc := &Proc{
		ctx: ctx,
		shutdown: hectoserver.ShutdownFunc(func() error {
			return hectoserver.ShutdownAll(shutdowners...)
		}),
	}

	for _, sconf := range config.Servers {
		h, shutdownFunc, err := hectoserver.CreateAndServe(ctx, &sconf)
		shutdowners = append(shutdowners, shutdownFunc)

		if err != nil {
			return proc, err
		}

		dnsServer := &dns.Server{
			Addr:    sconf.Listen,
			Net:     sconf.Proto,
			Handler: hectoserver.ServeDNS(h),
		}

		httpServer := &http.Server{
			Addr:    ":8080",
			Handler: hectoserver.ServeHTTP(h),
		}

		proc.dnsListeners = append(proc.dnsListeners, dnsServer)
		proc.httpListeners = append(proc.httpListeners, httpServer)
	}

	return proc, nil
}

func (p *Proc) Spawn() (err error) {
	log := zerolog.Ctx(p.ctx)

	// All listeners are blocking, therefore, spawn them in separate
	// routines, an wait until completion of all listeners.
	var (
		wg   sync.WaitGroup
		errs = make([]error, len(p.dnsListeners))
	)

	for i, ln := range p.dnsListeners {
		wg.Add(1)
		go func(i int, ln *dns.Server) {
			log.Info().Msgf("started (%s) at %s", ln.Net, ln.Addr)
			errs[i] = ln.ListenAndServe()
			wg.Done()
		}(i, ln)
	}

	for i, ln := range p.httpListeners {
		wg.Add(1)
		go func(i int, ln *http.Server) {
			log.Info().Msgf("started (http) at %s", ln.Addr)
			errs[i] = ln.ListenAndServe()
			wg.Done()
		}(i, ln)
	}

	wg.Wait()

	// Leave only the first non-nil error.
	for _, err := range errs {
		if err != nil && err != http.ErrServerClosed {
			return err
		}
	}
	return nil
}

func (p *Proc) Terminate() error {
	defer p.shutdown()

	for _, ln := range p.dnsListeners {
		ln.Shutdown()
	}
	for _, ln := range p.httpListeners {
		ln.Shutdown(context.TODO())
	}
	return nil
}

func main() {
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

		defer proc.Terminate()

		if err = proc.Spawn(); err != nil {
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
