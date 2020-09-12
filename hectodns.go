package main

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"

	"github.com/hectodns/hectodns/hectoserver"
)

type Proc struct {
	ctx      context.Context
	close    hectoserver.ShutdownFunc
	shutdown hectoserver.ShutdownFunc

	shutdownTimeout time.Duration

	configs   []hectoserver.ServerConfig
	listeners []hectoserver.Listener

	waitC chan struct{}
}

func NewProc(config *hectoserver.Config) (proc *Proc, err error) {
	var (
		closers     []hectoserver.ShutdownFunc
		shutdowners []hectoserver.ShutdownFunc
	)

	// Put the global logger into the context.
	ctx := log.Logger.WithContext(context.Background())
	proc = &Proc{
		ctx:   ctx,
		waitC: make(chan struct{}),
	}

	// Always configure shutdown and close functions.
	defer func() {
		proc.shutdown = hectoserver.MultiShutdown(shutdowners...)
		proc.close = hectoserver.MultiShutdown(closers...)
	}()

	proc.shutdownTimeout, err = time.ParseDuration(config.ServerShutdownTimeout)
	if err != nil {
		return proc, err
	}

	for _, conf := range config.Servers {
		var requestTimeout time.Duration

		if conf.RequestTimeout != "" {
			requestTimeout, err = time.ParseDuration(conf.RequestTimeout)
			if err != nil {
				return proc, err
			}
		}

		srv, err := hectoserver.CreateAndServe(ctx, &conf)

		closers = append(closers, srv.Close)
		shutdowners = append(shutdowners, srv.Shutdown)

		if err != nil {
			return proc, err
		}

		lc := hectoserver.ListenConfig{
			Addr:           conf.Listen,
			MaxConns:       conf.MaxConns,
			RequestTimeout: requestTimeout,
		}

		ln, err := hectoserver.Listen(conf.Proto, lc, srv.Handler)
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

	log.Info().Msgf("main process pid %d", os.Getpid())

	for i, ln := range p.listeners {
		wg.Add(1)
		go func(i int, ln hectoserver.Listener) {
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

func (p *Proc) Kill() error {
	defer close(p.waitC)
	defer p.close(p.ctx)

	for _, ln := range p.listeners {
		ln.Shutdown(p.ctx)
	}
	return nil
}

func (p *Proc) Quit() error {
	defer close(p.waitC)

	var (
		ctx    = p.ctx
		cancel context.CancelFunc
	)

	if p.shutdownTimeout != 0 {
		ctx, cancel = context.WithTimeout(p.ctx, p.shutdownTimeout)
		defer cancel()
	}

	defer p.shutdown(ctx)

	for _, ln := range p.listeners {
		ln.Shutdown(ctx)
	}
	return nil
}

func (p *Proc) Wait() error {
	<-p.waitC
	return nil
}

var (
	cmdTemplate = `{{.Usage}}

{{.Name}} options:{{range $index, $option := .VisibleFlags}}
	{{$option}}{{end}}
`
)

func main() {
	var opts struct {
		configFile string
	}

	// Configure console logger for the program.
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	actionFn := func(ctx *cli.Context) error {
		var proc *Proc

		termC := make(chan os.Signal, 1)
		signal.Notify(termC, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT)

		go func() {
			http.ListenAndServe("localhost:6060", nil)
		}()
		go func() {
			if sig := <-termC; proc != nil {
				log.Info().Msgf("received '%s' signal", sig)
				switch sig {
				case syscall.SIGKILL, syscall.SIGINT:
					proc.Kill()
				case syscall.SIGQUIT:
					proc.Quit()
				default:
					log.Info().Msgf("skipped '%s' signal", sig)
				}
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

		if err = proc.Spawn(); err != nil {
			log.Fatal().Msg(err.Error())
		}

		return proc.Wait()
	}

	cmd := cli.App{
		Name:   "hectodns",
		Usage:  "hectodns - a command to launch HectoDNS server",
		Action: actionFn,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "config-file",
				Aliases:     []string{"c"},
				Destination: &opts.configFile,
				Usage:       "hectodns-specific configuration `FILE`",
				Required:    true,
			},
		},
		CustomAppHelpTemplate: cmdTemplate,
	}

	if err := cmd.Run(os.Args); err != nil {
		log.Error().Msg(err.Error())
	}
}
