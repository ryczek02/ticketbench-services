package valkey_test

import (
	"context"
	"strings"
	"testing"

	"github.com/ryczek02/ticketbench-services/internal/engines/valkey"
	"github.com/ryczek02/ticketbench-services/internal/enginetest"
	"github.com/testcontainers/testcontainers-go"
	tcvalkey "github.com/testcontainers/testcontainers-go/modules/valkey"
)

func TestConformance(t *testing.T) {
	enginetest.SkipWithoutDocker(t)
	ctx := context.Background()
	c, err := tcvalkey.Run(ctx, "valkey/valkey:8.1")
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := valkey.Open(ctx, strings.TrimPrefix(uri, "redis://"), 100)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	enginetest.Run(t, eng)
}
