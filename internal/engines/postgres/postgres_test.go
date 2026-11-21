package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/ryczek02/ticketbench-services/internal/engines/postgres"
	"github.com/ryczek02/ticketbench-services/internal/enginetest"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestConformance(t *testing.T) {
	enginetest.SkipWithoutDocker(t)
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("ticketbench"),
		tcpostgres.WithUsername("ticketbench"),
		tcpostgres.WithPassword("ticketbench"),
		testcontainers.WithCmd("postgres", "-c", "max_connections=300", "-c", "fsync=off"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(2*time.Minute)),
	)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatal(err)
	}
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	eng, err := postgres.Open(ctx, dsn, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	enginetest.Run(t, eng)
}
