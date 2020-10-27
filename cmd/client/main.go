package main

import (
	"context"
	"github.com/uemuramikio/petal"
	"github.com/uemuramikio/petal/pb"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc/status"
	"os"
	"sort"
	"time"
)

const (
	version          = "0.0.1"
	etcdEndpointsKey = "etcd-endpoints"
	etcdTimeoutKey   = "etcd-timeout"
	etcdUsernameKey  = "etcd-username"
	etcdPasswordKey  = "etcd-password"
	logLevelKey      = "log-level"
	countKey         = "count"
	runCommand       = "run"
)

type petalConfig struct {
	logLevel      string
	etcdEndpoints []string
	etcdTimeout   time.Duration
	etcdUsername  string
	etcdPassword  string
}

func main() {
	pc := &petalConfig{}
	// init app
	app := initApp(pc)
	app.Command(runCommand).Action = func(c *cli.Context) error {
		if err := petal.SetLogLevel(pc.logLevel); err != nil {
			petal.Log.Error(err)
			return cli.Exit("set log level failed", 1)
		}
		pc.etcdEndpoints = c.StringSlice(etcdEndpointsKey)
		// Set up a connection to the server.
		holder := petal.EtcdHolder{
			EtcdOption: &petal.EtcdOption{
				Endpoints: pc.etcdEndpoints,
				Timeout:   pc.etcdTimeout,
				Username:  pc.etcdUsername,
				Password:  pc.etcdPassword,
			},
		}
		err := holder.Init()
		if err != nil {
			petal.Log.Errorf("init etcd holder failed, err=%v", err)
			os.Exit(1)
		}
		conn, err := holder.GetGrpcConnection()
		if err != nil {
			petal.Log.Errorf("get grpc connection failed, err=%v", err)
			os.Exit(1)
		}
		defer conn.Close()
		client := pb.NewGeneratorClient(conn)

		petal.Log.Info("start generate id")
		start := time.Now()
		count := c.Int(countKey)
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(count)*time.Second)
		for i := 0; i < count; i++ {
			// Contact the server and print out its response.
			//defer cancel()
			r, err := client.Generate(ctx, &pb.Request{})
			if err != nil {
				errStatus, _ := status.FromError(err)
				petal.Log.Errorf("could not generate[errorStatus=%s,errorMsg=%s]: %v", errStatus.Code(), errStatus.Message(), err)
				count = i
				break
			}
			petal.Log.Infof("ID %d : %d", i, r.GetId())
		}
		cancel()
		petal.Log.Debugf("time taken to generate [%d] ids is: %d ms", count, time.Since(start)/time.Millisecond)
		petal.Log.Infof("finish generate id,desired number of ids is: %d ,the actual number is: %d", c.Int(countKey), count)

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		petal.Log.Errorf("start app failed: %v", err)
	}
}

func initApp(config *petalConfig) *cli.App {
	app := &cli.App{
		Name:                 "petal client",
		Usage:                "connect to petal server and generate unique id",
		Version:              version,
		UsageText:            "main [global options] command [command options] [arguments...]\n   eg: client run --count=5",
		EnableBashCompletion: true,
		Commands: []*cli.Command{
			{
				Name:    runCommand,
				Aliases: []string{"r"},
				Usage:   "start the petal client",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        logLevelKey,
						Usage:       "log level (panic, fatal, error, warn, info)",
						Value:       "debug",
						Destination: &config.logLevel,
					},
					&cli.StringFlag{
						Name:        etcdUsernameKey,
						Usage:       "username for connecting etcd",
						Value:       "",
						Destination: &config.etcdUsername,
					},
					&cli.StringFlag{
						Name:        etcdPasswordKey,
						Usage:       "password for connecting etcd",
						Value:       "",
						Destination: &config.etcdPassword,
					},
					&cli.StringSliceFlag{
						Name:  etcdEndpointsKey,
						Usage: "the endpoints for connecting etcd",
						Value: cli.NewStringSlice("localhost:2379"),
					},
					&cli.DurationFlag{
						Name:        etcdTimeoutKey,
						Usage:       "etcd operation timeout(s)",
						Value:       5 * time.Second,
						Destination: &config.etcdTimeout,
					},
					&cli.IntFlag{
						Name:  countKey,
						Usage: "how many ids are generated",
						Value: 1,
					},
				},
			},
		},
	}

	sort.Sort(cli.FlagsByName(app.Flags))

	return app
}
