// Package mongo is the MongoDB adapter (mongo-driver v2, otelmongo).
//
// Document model:
//
//	obiekty      venue with embedded sectors
//	wydarzenia   event with embedded price list, denormalized city/venue/category names
//	bilety       ticket inventory, one document per seat and event
//	rezerwacje   reservation with embedded ticket snapshots and payment
//	klienci, organizatorzy, kategorie, kody_rabatowe
//
// Multi-document writes run in transactions, which need a replica set
// (a single-node "rs0" is enough).
package mongo

import (
	"context"
	"errors"
	"fmt"

	"github.com/ryczek02/ticketbench-services/internal/storekit"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.opentelemetry.io/contrib/instrumentation/go.mongodb.org/mongo-driver/v2/mongo/otelmongo"
)

var collections = []string{"kategorie", "organizatorzy", "obiekty", "wydarzenia", "klienci", "kody_rabatowe", "rezerwacje", "bilety"}

// Engine implements storekit.Engine.
type Engine struct {
	client *mongo.Client
	db     *mongo.Database
	txOpts *options.TransactionOptionsBuilder
}

// Open connects to MongoDB.
func Open(ctx context.Context, uri, database string, maxConns int) (*Engine, error) {
	opts := options.Client().ApplyURI(uri).
		SetMonitor(otelmongo.NewMonitor()).
		SetMaxPoolSize(uint64(maxConns)).
		SetMaxConnecting(64)
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}
	return &Engine{
		client: client,
		db:     client.Database(database),
		txOpts: options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority()),
	}, nil
}

func (e *Engine) Name() string { return "mongo" }

func (e *Engine) Strategies() []string { return []string{"atomic", "txn"} }

func (e *Engine) Close() error { return e.client.Disconnect(context.Background()) }

func (e *Engine) c(name string) *mongo.Collection { return e.db.Collection(name) }

func (e *Engine) ServerVersion(ctx context.Context) (string, error) {
	var res struct {
		Version string `bson:"version"`
	}
	err := e.client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&res)
	return "MongoDB " + res.Version, err
}

func (e *Engine) Reset(ctx context.Context) error {
	if err := e.db.Drop(ctx); err != nil {
		return err
	}
	// Create collections up front: implicit creation inside a transaction
	// would make the first transactional write slower than the rest.
	for _, c := range collections {
		if err := e.db.CreateCollection(ctx, c); err != nil {
			return err
		}
	}
	return nil
}

type index struct {
	coll string
	keys bson.D
}

var secondary = []index{
	{"bilety", bson.D{{Key: "wydarzenie_id", Value: 1}, {Key: "status", Value: 1}}},
	{"bilety", bson.D{{Key: "rezerwacja_id", Value: 1}}},
	{"rezerwacje", bson.D{{Key: "klient_id", Value: 1}, {Key: "utworzono", Value: -1}}},
	{"rezerwacje", bson.D{{Key: "status", Value: 1}, {Key: "wygasa_o", Value: 1}}},
	{"rezerwacje", bson.D{{Key: "utworzono", Value: 1}}},
	{"wydarzenia", bson.D{{Key: "miasto", Value: 1}, {Key: "data_rozpoczecia", Value: 1}}},
	{"wydarzenia", bson.D{{Key: "organizator_id", Value: 1}}},
	{"wydarzenia", bson.D{{Key: "data_rozpoczecia", Value: 1}}},
	{"wydarzenia", bson.D{{Key: "metadane.gatunek", Value: 1}, {Key: "metadane.tagi", Value: 1}}},
	{"obiekty", bson.D{{Key: "miasto", Value: 1}}},
}

func (e *Engine) SetIndexes(ctx context.Context, enabled bool) error {
	if !enabled {
		for _, c := range collections {
			if err := e.c(c).Indexes().DropAll(ctx); err != nil {
				return err
			}
		}
		return nil
	}
	for _, ix := range secondary {
		if _, err := e.c(ix.coll).Indexes().CreateOne(ctx, mongo.IndexModel{Keys: ix.keys}); err != nil {
			return fmt.Errorf("index on %s: %w", ix.coll, err)
		}
	}
	return nil
}

func (e *Engine) Stats(ctx context.Context) (storekit.Stats, error) {
	st := storekit.Stats{Rows: map[string]int64{}}
	var res struct {
		DataSize    float64 `bson:"dataSize"`
		StorageSize float64 `bson:"storageSize"`
		IndexSize   float64 `bson:"indexSize"`
		TotalSize   float64 `bson:"totalSize"`
	}
	if err := e.db.RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}}).Decode(&res); err != nil {
		return st, err
	}
	st.DataBytes, st.IndexBytes, st.TotalBytes = int64(res.StorageSize), int64(res.IndexSize), int64(res.TotalSize)
	for _, c := range collections {
		n, err := e.c(c).CountDocuments(ctx, bson.D{})
		if err != nil {
			return st, err
		}
		st.Rows[c] = n
	}
	return st, nil
}

// inTx runs fn in a multi-document transaction.
func (e *Engine) inTx(ctx context.Context, fn func(ctx context.Context) error) error {
	sess, err := e.client.StartSession()
	if err != nil {
		return err
	}
	defer sess.EndSession(ctx)
	if err := sess.StartTransaction(e.txOpts); err != nil {
		return err
	}
	sctx := mongo.NewSessionContext(ctx, sess)
	if err := fn(sctx); err != nil {
		_ = sess.AbortTransaction(context.WithoutCancel(ctx))
		return err
	}
	return sess.CommitTransaction(sctx)
}

// conflict maps WriteConflict and transient transaction errors.
func conflict(err error) error {
	if err == nil {
		return nil
	}
	var ce mongo.CommandError
	if errors.As(err, &ce) && (ce.Code == 112 || ce.HasErrorLabel("TransientTransactionError")) {
		return storekit.ErrConflict
	}
	var we mongo.WriteException
	if errors.As(err, &we) {
		for _, e := range we.WriteErrors {
			if e.Code == 112 {
				return storekit.ErrConflict
			}
		}
		if we.HasErrorLabel("TransientTransactionError") {
			return storekit.ErrConflict
		}
	}
	var le mongo.LabeledError
	if errors.As(err, &le) && le.HasErrorLabel("TransientTransactionError") {
		return storekit.ErrConflict
	}
	return err
}
