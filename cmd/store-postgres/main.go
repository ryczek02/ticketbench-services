// Command store-postgres serves the StoreService backed by PostgreSQL.
package main

import (
	"context"
	"fmt"

	"github.com/ryczek02/ticketbench-services/internal/engines/postgres"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
)

func main() {
	storekit.Main("store-postgres", func(ctx context.Context, profile string) (storekit.Engine, error) {
		// Profiles ("role", "tls", "tls_role") read TB_POSTGRES_DSN_<PROFILE>.
		dsn := storekit.Env("TB_POSTGRES_DSN", "postgres://ticketbench:ticketbench@localhost:5432/ticketbench?sslmode=disable")
		if profile != "" {
			dsn = storekit.Env("TB_POSTGRES_DSN"+storekit.ProfileEnvSuffix(profile), "")
			if dsn == "" {
				return nil, fmt.Errorf("TB_POSTGRES_DSN%s is not set", storekit.ProfileEnvSuffix(profile))
			}
		}
		return postgres.Open(ctx, dsn, storekit.EnvInt("TB_MAX_CONNS", 1100))
	})
}
