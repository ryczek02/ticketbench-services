// Command store-mariadb serves the StoreService backed by MariaDB.
package main

import (
	"context"
	"fmt"

	"github.com/ryczek02/ticketbench-services/internal/engines/mariadb"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
)

func main() {
	storekit.Main("store-mariadb", func(ctx context.Context, profile string) (storekit.Engine, error) {
		// Profiles ("role", "tls", "tls_role") read TB_MARIADB_DSN_<PROFILE>.
		dsn := storekit.Env("TB_MARIADB_DSN", "ticketbench:ticketbench@tcp(localhost:3306)/ticketbench")
		if profile != "" {
			dsn = storekit.Env("TB_MARIADB_DSN"+storekit.ProfileEnvSuffix(profile), "")
			if dsn == "" {
				return nil, fmt.Errorf("TB_MARIADB_DSN%s is not set", storekit.ProfileEnvSuffix(profile))
			}
		}
		return mariadb.Open(ctx, dsn, storekit.EnvInt("TB_MAX_CONNS", 1100))
	})
}
