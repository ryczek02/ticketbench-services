package storekit

import (
	"context"
	"sync/atomic"

	"github.com/ryczek02/ticketbench-contracts/pkg/datagen"
	"github.com/ryczek02/ticketbench-contracts/pkg/model"
	"golang.org/x/sync/errgroup"
)

// SalesBatch groups the generated sales of several events.
type SalesBatch struct {
	Tickets      []model.Ticket
	Reservations []model.Reservation
	Payments     []model.Payment
}

func (b *SalesBatch) reset() {
	b.Tickets = b.Tickets[:0]
	b.Reservations = b.Reservations[:0]
	b.Payments = b.Payments[:0]
}

// LoadCounts are the row counts written by ParallelSales.
type LoadCounts struct {
	Tickets, Reservations, Payments int64
}

// ParallelSales generates event sales on `workers` goroutines and calls
// flush with batches of at least `batch` tickets (the last batch may be
// smaller). Each worker owns its batch, so flush may keep no references.
func ParallelSales(ctx context.Context, d *datagen.Dataset, workers, batch int, flush func(context.Context, *SalesBatch) error) (LoadCounts, error) {
	g, ctx := errgroup.WithContext(ctx)
	events := make(chan int)
	var t, r, p atomic.Int64
	g.Go(func() error {
		defer close(events)
		for e := range d.Events {
			select {
			case events <- e:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	for range max(1, workers) {
		g.Go(func() error {
			b := &SalesBatch{}
			send := func() error {
				if len(b.Tickets) == 0 && len(b.Reservations) == 0 {
					return nil
				}
				if err := flush(ctx, b); err != nil {
					return err
				}
				t.Add(int64(len(b.Tickets)))
				r.Add(int64(len(b.Reservations)))
				p.Add(int64(len(b.Payments)))
				b.reset()
				return nil
			}
			for e := range events {
				s := d.Sales(e)
				b.Tickets = append(b.Tickets, s.Tickets...)
				b.Reservations = append(b.Reservations, s.Reservations...)
				b.Payments = append(b.Payments, s.Payments()...)
				if len(b.Tickets) >= batch {
					if err := send(); err != nil {
						return err
					}
				}
			}
			return send()
		})
	}
	err := g.Wait()
	return LoadCounts{t.Load(), r.Load(), p.Load()}, err
}

// ParallelCustomers generates customers in id ranges on `workers` goroutines.
func ParallelCustomers(ctx context.Context, d *datagen.Dataset, workers, batch int, flush func(context.Context, []model.Customer) error) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(max(1, workers))
	for from := int64(1); from <= d.Customers; from += int64(batch) {
		to := min(d.Customers, from+int64(batch)-1)
		g.Go(func() error {
			out := make([]model.Customer, 0, to-from+1)
			for id := from; id <= to; id++ {
				out = append(out, d.Customer(id))
			}
			return flush(ctx, out)
		})
	}
	return g.Wait()
}

// AllSeats returns every seat of every venue.
func AllSeats(d *datagen.Dataset) []model.Seat {
	out := make([]model.Seat, 0, d.Seats)
	for i := range d.Venues {
		datagen.Seats(&d.Venues[i], func(s model.Seat) { out = append(out, s) })
	}
	return out
}

// AllSectors returns every sector of every venue.
func AllSectors(d *datagen.Dataset) []model.Sector {
	var out []model.Sector
	for _, v := range d.Venues {
		out = append(out, v.Sectors...)
	}
	return out
}

// AllPrices returns the price lists of all events.
func AllPrices(d *datagen.Dataset) []model.Price {
	out := make([]model.Price, 0, d.Prices)
	for _, e := range d.Events {
		out = append(out, e.Prices...)
	}
	return out
}

// Chunks splits n items into [from, to) ranges of size at most size.
func Chunks(n, size int, fn func(from, to int) error) error {
	for from := 0; from < n; from += size {
		if err := fn(from, min(n, from+size)); err != nil {
			return err
		}
	}
	return nil
}
