package main

import (
	"context"
	"fmt"
	"github.com/uemuramikio/petal"
	"github.com/urfave/cli/v2"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
)

type petalConfig struct {
	datacenterID uint
	workerID     uint
	port         int
	logLevel     string
}

const (
	version         = "0.0.1"
	datacenterIDKey = "datacenter-id"
	workerIDKey     = "worker-id"
	portKey         = "port"
	logLevelKey     = "log-level"
	runCommand      = "run"
)

func main() {
	pc := &petalConfig{}
	cliApp := initApp(pc)
	cliApp.Command(runCommand).Action = func(c *cli.Context) error {
		if err := petal.SetLogLevel(pc.logLevel); err != nil {
			fmt.Println(err)
			return cli.Exit("set log level failed", 1)
		}

		var wg sync.WaitGroup
		ctx, cancel := context.WithCancel(context.Background())

		// main listener
		app, addr, err := newPetalApp(pc)
		if err != nil {
			fmt.Println(err)
			return cli.Exit("new petal app failed", 1)
		}

		wg.Add(1)
		go signalHandler(ctx, cancel, &wg, app)

		mainListener(ctx, app.Listen, addr)
		wg.Add(1)
		go serverServe(ctx, &wg, app.Serve)

		wg.Wait()
		return cli.Exit("shutdown completed", 0)
	}

	err := cliApp.Run(os.Args)
	if err != nil {
		log.Fatalf("start app failed: %v", err)
	}
}

func mainListener(ctx context.Context, fn petal.ListenFunc, addr string) {
	if err := fn(ctx, addr); err != nil {
		petal.Log.Errorf("Listen failed: %v", err)
		os.Exit(1)
	}
}

func serverServe(ctx context.Context, wg *sync.WaitGroup, fn petal.ServeFunc) {
	defer wg.Done()
	if err := fn(ctx); err != nil {
		petal.Log.Errorf("Serve failed: %v", err)
		os.Exit(1)
	}
}

func newPetalApp(peco *petalConfig) (*petal.App, string, error) {
	app, err := petal.NewApp(petal.Option{
		DatacenterID: peco.datacenterID,
		WorkerID:     peco.workerID,
	})
	if err != nil {
		return nil, "", err
	}
	return app, fmt.Sprintf(":%d", peco.port), nil
}

func signalHandler(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, app *petal.App) {
	defer wg.Done()
	trapSignals := []os.Signal{
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	}
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, trapSignals...)
	select {
	case sig := <-sigCh:
		petal.Log.Infof("Got signal %s", sig)
		app.Server.GracefulStop()
		_ = app.Listener.Close()
		cancel()
	case <-ctx.Done():
	}
}

func initApp(config *petalConfig) *cli.App {
	app := &cli.App{
		Name:                 "petal server",
		Usage:                "petal server for generating unique id",
		Version:              version,
		UsageText:            "main [global options] command [command options] [arguments...]\n   eg: server run --worker-id=0",
		EnableBashCompletion: true,
		Commands: []*cli.Command{
			{
				Name:    runCommand,
				Aliases: []string{"r"},
				Usage:   "start the petal server",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        logLevelKey,
						Usage:       "log level (panic, fatal, error, warn, info)",
						Value:       "debug",
						Destination: &config.logLevel,
						EnvVars:     convertFlagKeyToEnvKey(logLevelKey),
					},
					&cli.IntFlag{
						Name:        portKey,
						Usage:       "port to listen",
						Value:       50051,
						Destination: &config.port,
						EnvVars:     convertFlagKeyToEnvKey(portKey),
					},
					&cli.UintFlag{
						Name:        datacenterIDKey,
						Usage:       "datacenter id. must be unique",
						Value:       0,
						Destination: &config.datacenterID,
						EnvVars:     convertFlagKeyToEnvKey(datacenterIDKey),
					},
					&cli.UintFlag{
						Name:        workerIDKey,
						Usage:       "worker id. must be unique.",
						Value:       0,
						Destination: &config.workerID,
						EnvVars:     convertFlagKeyToEnvKey(workerIDKey),
					},
				},
			},
		},
	}

	sort.Sort(cli.FlagsByName(app.Flags))

	return app
}

func convertFlagKeyToEnvKey(flagKey string) []string {
	newKey := strings.Replace(flagKey, "-", "_", -1)
	return []string{
		strings.ToLower(newKey),
		strings.ToUpper(newKey),
	}
}
