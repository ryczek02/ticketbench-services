// Command store-mariadb serves the StoreService backed by MariaDB.
package main

import (
	"context"

	"github.com/ryczek02/ticketbench-services/internal/engines/mariadb"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
)

func main() {
	storekit.Main("store-mariadb", func(ctx context.Context) (storekit.Engine, error) {
		dsn := storekit.Env("TB_MARIADB_DSN", "ticketbench:ticketbench@tcp(localhost:3306)/ticketbench")
		return mariadb.Open(ctx, dsn, storekit.EnvInt("TB_MAX_CONNS", 1100))
	})
}
