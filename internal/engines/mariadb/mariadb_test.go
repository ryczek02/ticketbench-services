package mariadb_test

import (
	"context"
	"testing"

	"github.com/ryczek02/ticketbench-services/internal/engines/mariadb"
	"github.com/ryczek02/ticketbench-services/internal/enginetest"
	"github.com/testcontainers/testcontainers-go"
	tcmariadb "github.com/testcontainers/testcontainers-go/modules/mariadb"
)

func TestConformance(t *testing.T) {
	enginetest.SkipWithoutDocker(t)
	ctx := context.Background()
	c, err := tcmariadb.Run(ctx, "mariadb:11.8",
		tcmariadb.WithDatabase("ticketbench"),
		tcmariadb.WithUsername("ticketbench"),
		tcmariadb.WithPassword("ticketbench"),
		testcontainers.WithCmdArgs("--max-connections=300", "--innodb-flush-log-at-trx-commit=2"),
	)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatal(err)
	}
	dsn, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := mariadb.Open(ctx, dsn, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	enginetest.Run(t, eng)
}
