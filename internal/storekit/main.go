package storekit

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	tbv1 "github.com/ryczek02/ticketbench-contracts/gen/ticketbench/v1"
	"github.com/ryczek02/ticketbench-contracts/pkg/telemetry"
	"github.com/ryczek02/ticketbench-contracts/pkg/version"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// Env returns the environment variable or def.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// EnvInt returns the integer environment variable or def.
func EnvInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

// Opener connects to the database. It is retried until it succeeds or the
// startup timeout passes, because containers start in any order.
type Opener func(ctx context.Context) (Engine, error)

// Main runs a store service: telemetry, database connection, gRPC server
// with health checks and reflection, graceful shutdown on SIGTERM.
func Main(service string, open Opener) {
	if Subcommand(service, version.String()) {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, service, open); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, service string, open Opener) error {
	shutdown, err := telemetry.Setup(ctx, telemetry.Options{Service: service, Version: version.Version, Ratio: -1})
	if err != nil {
		return fmt.Errorf("telemetry: %w", err)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(sctx)
	}()

	eng, err := Connect(ctx, open, time.Duration(EnvInt("TB_STARTUP_TIMEOUT_S", 180))*time.Second)
	if err != nil {
		return err
	}
	defer eng.Close()

	addr := Env("TB_LISTEN", ":7001")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := grpc.NewServer(append(telemetry.GRPCServerOptions(), grpc.MaxRecvMsgSize(64<<20), grpc.MaxSendMsgSize(64<<20))...)
	tbv1.RegisterStoreServiceServer(srv, NewServer(eng))
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	reflection.Register(srv)

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(lis) }()
	slog.Info("store listening", "addr", addr, "engine", eng.Name(), "version", version.String())
	select {
	case <-ctx.Done():
		slog.Info("shutting down")
		hs.Shutdown()
		srv.GracefulStop()
		return nil
	case err := <-errc:
		return err
	}
}

// Connect retries open with backoff.
func Connect(ctx context.Context, open Opener, timeout time.Duration) (Engine, error) {
	deadline := time.Now().Add(timeout)
	wait := 500 * time.Millisecond
	for {
		eng, err := open(ctx)
		if err == nil {
			return eng, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("database not reachable after %s: %w", timeout, err)
		}
		slog.Warn("database not ready, retrying", "err", err, "in", wait.String())
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
		wait = min(wait*2, 5*time.Second)
	}
}
