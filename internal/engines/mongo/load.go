package mongo

import (
	"context"

	"github.com/ryczek02/ticketbench-contracts/pkg/datagen"
	"github.com/ryczek02/ticketbench-contracts/pkg/model"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// eventDoc denormalizes the venue city and names into the event so the
// common "events in a city" query needs no $lookup.
type eventDoc struct {
	model.Event  `bson:",inline"`
	City         string `bson:"miasto"`
	VenueName    string `bson:"obiekt_nazwa"`
	CategoryName string `bson:"kategoria_nazwa"`
}

func (e *Engine) eventDocs(d *datagen.Dataset, events []model.Event) []eventDoc {
	out := make([]eventDoc, len(events))
	for i, ev := range events {
		v := d.Venues[ev.VenueID-1]
		out[i] = eventDoc{Event: ev, City: v.City, VenueName: v.Name, CategoryName: d.Categories[ev.CategoryID-1].Name}
	}
	return out
}

const insertBatch = 5000

func insertAll[T any](ctx context.Context, c *mongo.Collection, docs []T) (int64, error) {
	var n int64
	err := storekit.Chunks(len(docs), insertBatch, func(from, to int) error {
		res, err := c.InsertMany(ctx, docs[from:to], options.InsertMany().SetOrdered(false))
		if res != nil {
			n += int64(len(res.InsertedIDs))
		}
		return err
	})
	return n, err
}

func (e *Engine) Load(ctx context.Context, d *datagen.Dataset, opts storekit.LoadOptions) (map[string]int64, error) {
	rows := map[string]int64{}
	var err error
	if rows["kategorie"], err = insertAll(ctx, e.c("kategorie"), d.Categories); err != nil {
		return nil, err
	}
	if rows["organizatorzy"], err = insertAll(ctx, e.c("organizatorzy"), d.Organizers); err != nil {
		return nil, err
	}
	if rows["obiekty"], err = insertAll(ctx, e.c("obiekty"), d.Venues); err != nil {
		return nil, err
	}
	if rows["wydarzenia"], err = insertAll(ctx, e.c("wydarzenia"), e.eventDocs(d, d.Events)); err != nil {
		return nil, err
	}
	if rows["kody_rabatowe"], err = insertAll(ctx, e.c("kody_rabatowe"), d.Discounts); err != nil {
		return nil, err
	}
	if err := storekit.ParallelCustomers(ctx, d, opts.Workers, insertBatch, func(ctx context.Context, cs []model.Customer) error {
		_, err := insertAll(ctx, e.c("klienci"), cs)
		return err
	}); err != nil {
		return nil, err
	}
	rows["klienci"] = d.Customers
	n, err := storekit.ParallelSales(ctx, d, opts.Workers, opts.BatchSize, func(ctx context.Context, b *storekit.SalesBatch) error {
		if _, err := insertAll(ctx, e.c("bilety"), b.Tickets); err != nil {
			return err
		}
		_, err := insertAll(ctx, e.c("rezerwacje"), b.Reservations)
		return err
	})
	if err != nil {
		return nil, err
	}
	rows["bilety"], rows["rezerwacje"], rows["platnosci"] = n.Tickets, n.Reservations, n.Payments
	return rows, nil
}
