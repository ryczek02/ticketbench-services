// Command store-valkey serves the StoreService backed by Valkey.
package main

import (
	"context"
	"fmt"

	"github.com/ryczek02/ticketbench-services/internal/engines/valkey"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
)

func main() {
	storekit.Main("store-valkey", func(ctx context.Context, profile string) (storekit.Engine, error) {
		// Profiles ("role", "tls", "tls_role") read TB_VALKEY_ADDR_<PROFILE>.
		dsn := storekit.Env("TB_VALKEY_ADDR", "localhost:6379")
		if profile != "" {
			dsn = storekit.Env("TB_VALKEY_ADDR"+storekit.ProfileEnvSuffix(profile), "")
			if dsn == "" {
				return nil, fmt.Errorf("TB_VALKEY_ADDR%s is not set", storekit.ProfileEnvSuffix(profile))
			}
		}
		return valkey.Open(ctx, dsn, storekit.EnvInt("TB_MAX_CONNS", 1100))
	})
}
