package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/hectodns/hectodns/hectoserver"
)

type Proc struct {
	ctx      context.Context
	shutdown hectoserver.ShutdownFunc

	configs   []hectoserver.ServerConfig
	listeners []hectoserver.Listener
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

	for _, conf := range config.Servers {
		h, shutdownFunc, err := hectoserver.CreateAndServe(ctx, &conf)
		shutdowners = append(shutdowners, shutdownFunc)
		if err != nil {
			return proc, err
		}

		ln, err := hectoserver.Listen(conf.Proto, conf.Listen, h)
		if err != nil {
			return proc, err
		}

		proc.configs = append(proc.configs, conf)
		proc.listeners = append(proc.listeners, ln)
	}

	return proc, nil
}

func (p *Proc) Spawn() (err error) {
	log := zerolog.Ctx(p.ctx)

	// All listeners are blocking, therefore, spawn them in separate
	// routines, an wait until completion of all listeners.
	var (
		wg   sync.WaitGroup
		errs = make([]error, len(p.listeners))
	)

	for i, ln := range p.listeners {
		wg.Add(1)
		go func(i int, ln hectoserver.Listener) {
			log.Info().Msgf("started (%s) at %s", p.configs[i].Proto, p.configs[i].Listen)
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

	for _, ln := range p.listeners {
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
