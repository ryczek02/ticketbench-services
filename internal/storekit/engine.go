// Package storekit turns an Engine (one database adapter) into a gRPC
// StoreService: it derives scenario parameters, measures server-side time,
// runs the contention test and records spans and metrics.
package storekit

import (
	"context"
	"errors"
	"time"

	"github.com/ryczek02/ticketbench-contracts/pkg/datagen"
	"github.com/ryczek02/ticketbench-contracts/pkg/scenario"
)

// ErrConflict marks a retryable concurrency failure: serialization failure,
// deadlock, lock wait refused (NOWAIT) or an aborted WATCH transaction.
var ErrConflict = errors.New("concurrency conflict")

// LoadOptions tune bulk loading.
type LoadOptions struct {
	BatchSize int
	Workers   int
}

// Plan is the output of Explain.
type Plan struct {
	Format    string // text, json or none
	Plan      string
	Statement string
}

// Stats describe storage use.
type Stats struct {
	DataBytes  int64
	IndexBytes int64
	TotalBytes int64
	Rows       map[string]int64
}

// Booking is one attempt of the contention test: take ticket TicketID for a
// new reservation ReservationID.
type Booking struct {
	TicketID      int64
	EventID       int64
	ReservationID int64
	CustomerID    int64
	Price         float64
	At            time.Time
}

// Engine is implemented by each database adapter.
type Engine interface {
	Name() string
	ServerVersion(ctx context.Context) (string, error)
	// Strategies lists booking strategies for the contention test; the
	// first one is the default.
	Strategies() []string

	Reset(ctx context.Context) error
	Load(ctx context.Context, d *datagen.Dataset, opts LoadOptions) (map[string]int64, error)
	SetIndexes(ctx context.Context, enabled bool) error

	// Prepare inserts the scenario fixture. It is not timed.
	Prepare(ctx context.Context, p scenario.Params) error
	// Run executes the measured operation and returns the affected or
	// returned row count.
	Run(ctx context.Context, p scenario.Params) (int64, error)
	Explain(ctx context.Context, p scenario.Params) (Plan, error)

	// ResetHot frees the hot tickets and removes reservations created by
	// earlier contention rounds (ids >= scenario.ContentionReservationBase).
	ResetHot(ctx context.Context, d *datagen.Dataset, tickets []int64) error
	// TryBook returns true when the ticket was free and is now reserved,
	// false when it was already taken, ErrConflict for retryable failures.
	TryBook(ctx context.Context, strategy string, b Booking) (bool, error)
	// VerifyHot returns the number of hot tickets in reserved state and the
	// number of contention reservations that exist.
	VerifyHot(ctx context.Context, tickets []int64) (sold, reservations int64, err error)

	Stats(ctx context.Context) (Stats, error)
	Close() error
}
