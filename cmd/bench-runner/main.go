// Command bench-runner serves RunnerService: it receives a benchmark plan
// from the tbench CLI, executes it against the store services and streams
// the results back. Trace context from the CLI is continued in every call.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	tbv1 "github.com/ryczek02/ticketbench-contracts/gen/ticketbench/v1"
	"github.com/ryczek02/ticketbench-contracts/pkg/telemetry"
	"github.com/ryczek02/ticketbench-contracts/pkg/version"
	"github.com/ryczek02/ticketbench-services/internal/runner"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

const defaultStores = "postgres=store-postgres:7001,mariadb=store-mariadb:7002,mongo=store-mongo:7003,valkey=store-valkey:7004"

func main() {
	if storekit.Subcommand("bench-runner", version.String()) {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	shutdown, err := telemetry.Setup(ctx, telemetry.Options{Service: "bench-runner", Version: version.Version, Ratio: -1})
	if err != nil {
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(sctx)
	}()

	cfg, err := runner.ParseStores(storekit.Env("TB_STORES", defaultStores))
	if err != nil {
		return err
	}
	stores, err := runner.Dial(cfg)
	if err != nil {
		return err
	}
	srv := runner.NewServer(stores)
	defer srv.Close()

	addr := storekit.Env("TB_LISTEN", ":7000")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	g := grpc.NewServer(append(telemetry.GRPCServerOptions(),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	)...)
	tbv1.RegisterRunnerServiceServer(g, srv)
	hs := health.NewServer()
	healthpb.RegisterHealthServer(g, hs)
	reflection.Register(g)

	errc := make(chan error, 1)
	go func() { errc <- g.Serve(lis) }()
	slog.Info("runner listening", "addr", addr, "stores", cfg, "version", version.String())
	select {
	case <-ctx.Done():
		hs.Shutdown()
		g.GracefulStop()
		return nil
	case err := <-errc:
		return err
	}
}
