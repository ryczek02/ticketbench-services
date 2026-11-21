package storekit

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Subcommand handles "version" and "healthcheck" arguments shared by all
// service binaries. It returns true when the process should exit.
// The images are distroless (no shell, no curl), so Docker health checks
// run the binary itself: ["CMD", "/app", "healthcheck"].
func Subcommand(service, version string) bool {
	if len(os.Args) < 2 {
		return false
	}
	switch os.Args[1] {
	case "version", "--version":
		fmt.Println(service, version)
		return true
	case "healthcheck":
		addr := Env("TB_LISTEN", ":7001")
		if strings.HasPrefix(addr, ":") {
			addr = "127.0.0.1" + addr
		}
		if err := checkHealth(addr); err != nil {
			fmt.Fprintln(os.Stderr, "unhealthy:", err)
			os.Exit(1)
		}
		return true
	}
	return false
}

func checkHealth(addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	res, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if res.Status != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("status %s", res.Status)
	}
	return nil
}
