package mongo

import (
	"context"
	"time"

	"github.com/ryczek02/ticketbench-contracts/pkg/datagen"
	"github.com/ryczek02/ticketbench-contracts/pkg/model"
	"github.com/ryczek02/ticketbench-contracts/pkg/scenario"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
)

func (e *Engine) ResetHot(ctx context.Context, _ *datagen.Dataset, tickets []int64) error {
	if _, err := e.c("rezerwacje").DeleteMany(ctx, M{"_id": M{"$gt": scenario.ContentionReservationBase}}); err != nil {
		return err
	}
	_, err := e.c("bilety").UpdateMany(ctx, M{"_id": M{"$in": tickets}},
		D{{Key: "$set", Value: M{"status": model.TicketFree}}, {Key: "$unset", Value: M{"rezerwacja_id": "", "zablokowany_do": ""}}})
	return err
}

func (e *Engine) VerifyHot(ctx context.Context, tickets []int64) (sold, reservations int64, err error) {
	sold, err = e.c("bilety").CountDocuments(ctx, M{"_id": M{"$in": tickets}, "status": model.TicketReserved, "rezerwacja_id": M{"$gt": scenario.ContentionReservationBase}})
	if err != nil {
		return
	}
	reservations, err = e.c("rezerwacje").CountDocuments(ctx, M{"_id": M{"$gt": scenario.ContentionReservationBase}})
	return
}

// TryBook strategies:
//
//	atomic  conditional updateOne on the ticket (document-level atomicity),
//	        then insertOne of the reservation, no transaction
//	txn     both writes in one multi-document transaction; a WriteConflict
//	        aborts it and counts as a conflict
func (e *Engine) TryBook(ctx context.Context, strategy string, b storekit.Booking) (bool, error) {
	exp := b.At.Add(15 * time.Minute)
	r := model.Reservation{
		ID: b.ReservationID, CustomerID: b.CustomerID, CreatedAt: b.At, Status: model.ReservationPending,
		ExpiresAt: &exp, Amount: b.Price, Meta: model.ReservationMeta{Channel: "web"},
		Tickets: []model.TicketRef{{TicketID: b.TicketID, EventID: b.EventID, Price: b.Price}},
	}
	book := func(ctx context.Context) (bool, error) {
		res, err := e.c("bilety").UpdateOne(ctx, M{"_id": b.TicketID, "status": model.TicketFree},
			M{"$set": M{"status": model.TicketReserved, "rezerwacja_id": b.ReservationID}})
		if err != nil || res.ModifiedCount == 0 {
			return false, err
		}
		_, err = e.c("rezerwacje").InsertOne(ctx, r)
		return err == nil, err
	}
	if strategy == "atomic" {
		ok, err := book(ctx)
		return ok, conflict(err)
	}
	var ok bool
	err := e.inTx(ctx, func(ctx context.Context) error {
		var err error
		ok, err = book(ctx)
		return err
	})
	if err != nil {
		return false, conflict(err)
	}
	return ok, nil
}
