// Command store-valkey serves the StoreService backed by Valkey.
package main

import (
	"context"

	"github.com/ryczek02/ticketbench-services/internal/engines/valkey"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
)

func main() {
	storekit.Main("store-valkey", func(ctx context.Context) (storekit.Engine, error) {
		return valkey.Open(ctx, storekit.Env("TB_VALKEY_ADDR", "localhost:6379"), storekit.EnvInt("TB_MAX_CONNS", 1100))
	})
}
