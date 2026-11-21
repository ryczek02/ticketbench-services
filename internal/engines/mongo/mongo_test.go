package mongo_test

import (
	"context"
	"strings"
	"testing"

	"github.com/ryczek02/ticketbench-services/internal/engines/mongo"
	"github.com/ryczek02/ticketbench-services/internal/enginetest"
	"github.com/testcontainers/testcontainers-go"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
)

func TestConformance(t *testing.T) {
	enginetest.SkipWithoutDocker(t)
	ctx := context.Background()
	c, err := tcmongo.Run(ctx, "mongo:8.0", tcmongo.WithReplicaSet("rs0"))
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The replica set advertises the container IP, which the host cannot
	// reach on Docker Desktop; talk to the single member directly.
	if strings.Contains(uri, "?") {
		uri += "&directConnection=true"
	} else {
		uri += "/?directConnection=true"
	}
	eng, err := mongo.Open(ctx, uri, "ticketbench", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	enginetest.Run(t, eng)
}
