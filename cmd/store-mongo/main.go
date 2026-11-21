// Command store-mongo serves the StoreService backed by MongoDB.
package main

import (
	"context"
	"fmt"

	"github.com/ryczek02/ticketbench-services/internal/engines/mongo"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
)

func main() {
	storekit.Main("store-mongo", func(ctx context.Context, profile string) (storekit.Engine, error) {
		// Profiles ("role", "tls", "tls_role") read TB_MONGO_URI_<PROFILE>.
		dsn := storekit.Env("TB_MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0")
		if profile != "" {
			dsn = storekit.Env("TB_MONGO_URI"+storekit.ProfileEnvSuffix(profile), "")
			if dsn == "" {
				return nil, fmt.Errorf("TB_MONGO_URI%s is not set", storekit.ProfileEnvSuffix(profile))
			}
		}
		return mongo.Open(ctx, dsn, storekit.Env("TB_MONGO_DB", "ticketbench"), storekit.EnvInt("TB_MAX_CONNS", 1100))
	})
}
