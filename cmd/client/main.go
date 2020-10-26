package main

import (
	"context"
	"github.com/uemuramikio/petal"
	"github.com/uemuramikio/petal/pb"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"os"
	"sort"
	"time"
)

const (
	version     = "0.0.1"
	endpointKey = "endpoint"
	countKey    = "count"
	runCommand  = "run"
)

func main() {
	// init app
	app := initApp()
	app.Command(runCommand).Action = func(c *cli.Context) error {
		// Set up a connection to the server.
		conn, err := grpc.Dial(c.String(endpointKey), grpc.WithInsecure())
		if err != nil {
			petal.Log.Errorf("did not connect: %v", err)
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

func initApp() *cli.App {
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
						Name:  endpointKey,
						Usage: "endpoint for petal server,host:port format",
						Value: "localhost:50051",
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
