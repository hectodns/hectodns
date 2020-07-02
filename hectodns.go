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
	"github.com/urfave/cli/v2"

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

	// Put the global logger into the context.
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

		return nil
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
		fmt.Println(err)
	}
}
