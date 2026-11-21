// Command store-postgres serves the StoreService backed by PostgreSQL.
package main

import (
	"context"

	"github.com/ryczek02/ticketbench-services/internal/engines/postgres"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
)

func main() {
	storekit.Main("store-postgres", func(ctx context.Context) (storekit.Engine, error) {
		dsn := storekit.Env("TB_POSTGRES_DSN", "postgres://ticketbench:ticketbench@localhost:5432/ticketbench?sslmode=disable")
		return postgres.Open(ctx, dsn, storekit.EnvInt("TB_MAX_CONNS", 1100))
	})
}
