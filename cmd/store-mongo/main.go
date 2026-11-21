// Command store-mongo serves the StoreService backed by MongoDB.
package main

import (
	"context"

	"github.com/ryczek02/ticketbench-services/internal/engines/mongo"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
)

func main() {
	storekit.Main("store-mongo", func(ctx context.Context) (storekit.Engine, error) {
		uri := storekit.Env("TB_MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0")
		return mongo.Open(ctx, uri, storekit.Env("TB_MONGO_DB", "ticketbench"), storekit.EnvInt("TB_MAX_CONNS", 1100))
	})
}
