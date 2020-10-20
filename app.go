package petal

import (
	"context"
	"fmt"
	"github.com/uemuramikio/petal/pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	stdlog "log"
	"net"
	"time"
)

var (
	// Version number
	Version   = "development"
	logger, _ = zap.NewDevelopment()
	Log       = logger.Sugar()
)

// App is main struct of the Application
type App struct {
	Listener  net.Listener
	Server    *grpc.Server
	gen       Generator
	startedAt time.Time
}

// Option represents a App optional parameters.
type Option struct {
	DatacenterID uint
	WorkerID     uint
}

// NewApp create and returns new App instance.
func NewApp(opt Option) (*App, error) {
	gen, err := NewGenerator(opt.DatacenterID, opt.WorkerID)
	if err != nil {
		return nil, err
	}

	return &App{
		gen:       gen,
		startedAt: time.Now(),
	}, nil
}

// SetLogLevel sets log level.
// Log level must be one of debug, info, warning, error, fatal and panic.
func SetLogLevel(str string) error {
	conf := zap.Config{
		Encoding:         "console",
		EncoderConfig:    zap.NewDevelopmentEncoderConfig(),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	switch str {
	case "debug":
		conf.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		conf.Development = true
	case "info":
		conf.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warning":
		conf.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		conf.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	case "fatal":
		conf.Level = zap.NewAtomicLevelAt(zap.FatalLevel)
	case "panic":
		conf.Level = zap.NewAtomicLevelAt(zap.PanicLevel)
	default:
		return fmt.Errorf("invalid log level %s", str)
	}
	_ = logger.Sync()
	logger, _ = conf.Build()
	Log = logger.Sugar()
	return nil
}

// StdLogger returns the standard logger.
func StdLogger() *stdlog.Logger {
	return zap.NewStdLog(logger)
}

// ListenFunc is the type for listeners.
type ListenFunc func(context.Context, string) error

// Listen starts listen on host:port
func (app *App) Listen(ctx context.Context, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	app.Listener = lis
	Log.Infof("Listening at %s", lis.Addr().String())
	Log.Infof("Datacenter ID = %d | Worker ID = %d", app.gen.DatacenterID(), app.gen.WorkerID())

	return nil
}

type ServeFunc func(context.Context) error

func (app *App) Serve(ctx context.Context) error {
	s := grpc.NewServer()
	pb.RegisterGeneratorServer(s, &server{gen: app.gen})
	app.Server = s

	if err := s.Serve(app.Listener); err != nil {
		Log.Warnf("failed to serve: %v", err)
		return err
	}

	return nil
}

// server is used to implement petal.GeneratorServer
type server struct {
	pb.UnimplementedGeneratorServer
	gen Generator
}

// Generate implements petal.GeneratorServer
func (s *server) Generate(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	id, err := s.NextID()
	Log.Debugf("Generated ID: %d", id)
	return &pb.Reply{Id: id}, err
}

// NextID generates new ID.
func (s *server) NextID() (uint64, error) {
	id, err := s.gen.NextID()
	return id, err
}
