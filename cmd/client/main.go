package main

import (
	"context"
	"fmt"
	"github.com/uemuramikio/petal/pb"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"log"
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
			log.Fatalf("did not connect: %v", err)
		}
		defer conn.Close()
		client := pb.NewGeneratorClient(conn)

		fmt.Printf("start generate id\n")
		count := c.Int(countKey)
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(count)*time.Second)
		for i := 0; i < count; i++ {
			// Contact the server and print out its response.
			//defer cancel()
			r, err := client.Generate(ctx, &pb.Request{})
			if err != nil {
				errStatus, _ := status.FromError(err)
				fmt.Println(errStatus.Message())
				fmt.Println(errStatus.Code())
				log.Fatalf("could not generate: %v", err)
			}
			fmt.Printf("ID %d : %d\n", i, r.GetId())
		}
		cancel()
		fmt.Printf("finish generate id,total : %d\nexit now.\n", count)

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalf("start app failed: %v", err)
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
