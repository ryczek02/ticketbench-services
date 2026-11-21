package mongo

import (
	"context"
	"fmt"
	"sync"

	"github.com/ryczek02/ticketbench-contracts/pkg/model"
	"github.com/ryczek02/ticketbench-contracts/pkg/scenario"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type M = bson.M
type D = bson.D

var venueCache sync.Map // venue id -> eventDoc template

// fixtureDocs fills the denormalized fields of fixture events from the
// obiekty and kategorie collections.
func (e *Engine) fixtureDocs(ctx context.Context, events []model.Event) ([]eventDoc, error) {
	out := make([]eventDoc, len(events))
	for i, ev := range events {
		tmpl, ok := venueCache.Load(ev.VenueID)
		if !ok {
			var v model.Venue
			if err := e.c("obiekty").FindOne(ctx, M{"_id": ev.VenueID}).Decode(&v); err != nil {
				return nil, fmt.Errorf("venue %d: %w", ev.VenueID, err)
			}
			var c model.Category
			if err := e.c("kategorie").FindOne(ctx, M{"_id": ev.CategoryID}).Decode(&c); err != nil {
				return nil, err
			}
			tmpl = eventDoc{City: v.City, VenueName: v.Name, CategoryName: c.Name}
			venueCache.Store(ev.VenueID, tmpl)
		}
		d := tmpl.(eventDoc)
		d.Event = ev
		out[i] = d
	}
	return out, nil
}

func ticketIDs(r *model.Reservation) []int64 {
	out := make([]int64, len(r.Tickets))
	for i, t := range r.Tickets {
		out[i] = t.TicketID
	}
	return out
}

var release = D{{Key: "$set", Value: M{"status": model.TicketFree}}, {Key: "$unset", Value: M{"rezerwacja_id": ""}}}

func count(ctx context.Context, cur *mongo.Cursor, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	defer cur.Close(ctx)
	var n int64
	for cur.Next(ctx) {
		n++
	}
	return n, cur.Err()
}

func (e *Engine) Run(ctx context.Context, p scenario.Params) (int64, error) {
	tickets, reservations, events := e.c("bilety"), e.c("rezerwacje"), e.c("wydarzenia")
	switch p.ID {
	case "C1", "C2":
		var n int64
		err := e.inTx(ctx, func(ctx context.Context) error {
			if _, err := reservations.InsertOne(ctx, p.Reservation); err != nil {
				return err
			}
			res, err := tickets.UpdateMany(ctx, M{"_id": M{"$in": p.TicketIDs}, "status": model.TicketFree},
				M{"$set": M{"status": model.TicketReserved, "rezerwacja_id": p.Reservation.ID}})
			if err != nil {
				return err
			}
			if n = res.ModifiedCount; n != int64(len(p.TicketIDs)) {
				return fmt.Errorf("%s: reserved %d of %d tickets", p.ID, n, len(p.TicketIDs))
			}
			return nil
		})
		return n, err
	case "C3":
		return insertAll(ctx, tickets, p.Tickets)
	case "C4":
		_, err := e.c("klienci").InsertOne(ctx, p.Customer)
		return 1, err
	case "C5":
		docs, err := e.fixtureDocs(ctx, []model.Event{*p.Event})
		if err != nil {
			return 0, err
		}
		_, err = events.InsertOne(ctx, docs[0])
		return 1, err
	case "C6":
		res, err := reservations.UpdateOne(ctx, M{"_id": p.ReservationID},
			D{{Key: "$set", Value: M{"status": model.ReservationPaid, "platnosc": p.Payment}}, {Key: "$unset", Value: M{"wygasa_o": ""}}})
		if err != nil {
			return 0, err
		}
		return res.ModifiedCount, nil
	case "R1":
		var r model.Reservation
		err := reservations.FindOne(ctx, M{"_id": p.ReservationID}).Decode(&r)
		if err == mongo.ErrNoDocuments {
			return 0, nil
		}
		return int64(max(1, len(r.Tickets))), err
	case "R2":
		cur, err := tickets.Find(ctx, M{"wydarzenie_id": p.EventID, "status": model.TicketFree},
			options.Find().SetSort(D{{Key: "sektor_id", Value: 1}, {Key: "rzad", Value: 1}, {Key: "numer", Value: 1}}).
				SetProjection(M{"sektor_id": 1, "rzad": 1, "numer": 1, "cena": 1}))
		return count(ctx, cur, err)
	case "R3":
		cur, err := events.Find(ctx, M{"miasto": p.City, "data_rozpoczecia": M{"$gte": p.From, "$lt": p.To}},
			options.Find().SetSort(M{"data_rozpoczecia": 1}).SetProjection(M{"nazwa": 1, "data_rozpoczecia": 1, "obiekt_nazwa": 1, "kategoria_nazwa": 1}))
		return count(ctx, cur, err)
	case "R4":
		cur, err := reservations.Find(ctx, M{"klient_id": p.CustomerID},
			options.Find().SetSort(M{"utworzono": -1}).SetLimit(50).SetProjection(M{"utworzono": 1, "status": 1, "kwota": 1, "bilety.bilet_id": 1}))
		return count(ctx, cur, err)
	case "R5":
		var ids []int64
		if err := distinct(ctx, events, "_id", M{"organizator_id": p.OrganizerID}, &ids); err != nil {
			return 0, err
		}
		cur, err := tickets.Aggregate(ctx, mongo.Pipeline{
			{{Key: "$match", Value: M{"wydarzenie_id": M{"$in": ids}, "status": model.TicketSold}}},
			{{Key: "$group", Value: M{"_id": "$wydarzenie_id", "sprzedane": M{"$sum": 1}, "przychod": M{"$sum": "$cena"}}}},
		})
		if _, err := count(ctx, cur, err); err != nil {
			return 0, err
		}
		return int64(len(ids)), nil
	case "R6":
		cur, err := tickets.Aggregate(ctx, top10Pipeline)
		return count(ctx, cur, err)
	case "U1":
		res, err := reservations.UpdateOne(ctx, M{"_id": p.ReservationID},
			D{{Key: "$set", Value: M{"status": model.ReservationPaid}}, {Key: "$unset", Value: M{"wygasa_o": ""}}})
		if err != nil {
			return 0, err
		}
		return res.MatchedCount, nil
	case "U2":
		res, err := tickets.UpdateOne(ctx, M{"_id": p.TicketIDs[0], "status": model.TicketFree},
			M{"$set": M{"status": model.TicketHeld, "zablokowany_do": p.NewDate}})
		if err != nil {
			return 0, err
		}
		if res.ModifiedCount != 1 {
			return 0, fmt.Errorf("U2: ticket %d not free", p.TicketIDs[0])
		}
		return 1, nil
	case "U3":
		var n int64
		err := e.inTx(ctx, func(ctx context.Context) error {
			res, err := tickets.UpdateMany(ctx, M{"wydarzenie_id": p.EventID, "sektor_id": p.SectorID, "status": model.TicketFree},
				mongo.Pipeline{{{Key: "$set", Value: M{"cena": M{"$round": bson.A{M{"$multiply": bson.A{"$cena", p.Factor}}, 2}}}}}})
			if err != nil {
				return err
			}
			n = res.ModifiedCount
			_, err = events.UpdateOne(ctx, M{"_id": p.EventID, "cennik.sektor_id": p.SectorID}, M{"$mul": M{"cennik.$.cena": p.Factor}})
			return err
		})
		return n, err
	case "U4":
		c := p.Customer
		res, err := e.c("klienci").UpdateOne(ctx, M{"_id": c.ID}, M{"$set": M{
			"email": c.Email, "telefon": c.Phone, "preferencje.jezyk": c.Prefs.Language, "preferencje.newsletter": c.Prefs.Newsletter,
		}})
		if err != nil {
			return 0, err
		}
		return res.MatchedCount, nil
	case "U5":
		res, err := events.UpdateOne(ctx, M{"_id": p.EventID}, M{"$set": M{"data_rozpoczecia": p.NewDate}})
		if err != nil {
			return 0, err
		}
		return res.MatchedCount, nil
	case "U6":
		var n int64
		err := e.inTx(ctx, func(ctx context.Context) error {
			var expired []model.Reservation
			cur, err := reservations.Find(ctx, M{"status": model.ReservationPending, "wygasa_o": M{"$lt": p.AsOf}},
				options.Find().SetProjection(M{"bilety.bilet_id": 1}))
			if err != nil {
				return err
			}
			if err := cur.All(ctx, &expired); err != nil {
				return err
			}
			var resIDs, tIDs []int64
			for i := range expired {
				resIDs = append(resIDs, expired[i].ID)
				tIDs = append(tIDs, ticketIDs(&expired[i])...)
			}
			// Ticket ids come from the embedded snapshots, so the update
			// uses the _id index even without secondary indexes.
			res, err := tickets.UpdateMany(ctx, M{"_id": M{"$in": tIDs}}, release)
			if err != nil {
				return err
			}
			n = res.ModifiedCount
			_, err = reservations.UpdateMany(ctx, M{"_id": M{"$in": resIDs}}, M{"$set": M{"status": model.ReservationExpired}})
			return err
		})
		if err == nil && n != scenario.BulkReservations {
			err = fmt.Errorf("U6: released %d tickets, want %d", n, scenario.BulkReservations)
		}
		return n, err
	case "D1", "D2":
		var n int64
		err := e.inTx(ctx, func(ctx context.Context) error {
			var r model.Reservation
			err := reservations.FindOneAndDelete(ctx, M{"_id": p.ReservationID}).Decode(&r)
			if err == mongo.ErrNoDocuments {
				return nil
			}
			if err != nil {
				return err
			}
			n = 1
			if len(r.Tickets) == 0 {
				return nil
			}
			if p.ID == "D1" {
				_, err = tickets.UpdateMany(ctx, M{"_id": M{"$in": ticketIDs(&r)}}, release)
			} else {
				_, err = tickets.DeleteMany(ctx, M{"_id": M{"$in": ticketIDs(&r)}})
			}
			return err
		})
		return n, err
	case "D3":
		res, err := reservations.DeleteMany(ctx, M{"status": model.ReservationExpired, "wygasa_o": M{"$lt": p.Cutoff}})
		if err != nil {
			return 0, err
		}
		return res.DeletedCount, nil
	case "D4":
		var n int64
		err := e.inTx(ctx, func(ctx context.Context) error {
			var rs []model.Reservation
			cur, err := reservations.Find(ctx, M{"klient_id": p.CustomerID}, options.Find().SetProjection(M{"bilety.bilet_id": 1}))
			if err != nil {
				return err
			}
			if err := cur.All(ctx, &rs); err != nil {
				return err
			}
			var tIDs []int64
			for i := range rs {
				tIDs = append(tIDs, ticketIDs(&rs[i])...)
			}
			if len(tIDs) > 0 {
				if _, err := tickets.UpdateMany(ctx, M{"_id": M{"$in": tIDs}}, release); err != nil {
					return err
				}
			}
			res, err := reservations.DeleteMany(ctx, M{"klient_id": p.CustomerID})
			if err != nil {
				return err
			}
			n = res.DeletedCount
			_, err = e.c("klienci").DeleteOne(ctx, M{"_id": p.CustomerID})
			return err
		})
		return n, err
	case "D5":
		var n int64
		err := e.inTx(ctx, func(ctx context.Context) error {
			res, err := tickets.DeleteMany(ctx, M{"wydarzenie_id": p.EventID})
			if err != nil {
				return err
			}
			n = res.DeletedCount
			_, err = events.DeleteOne(ctx, M{"_id": p.EventID, "status": model.EventCancelled})
			return err
		})
		return n, err
	case "D6":
		var n int64
		err := e.inTx(ctx, func(ctx context.Context) error {
			var ids []int64
			if err := distinct(ctx, events, "_id", M{"data_rozpoczecia": M{"$lt": p.Cutoff}}, &ids); err != nil {
				return err
			}
			if _, err := reservations.DeleteMany(ctx, M{"utworzono": M{"$lt": p.Cutoff}}); err != nil {
				return err
			}
			res, err := tickets.DeleteMany(ctx, M{"wydarzenie_id": M{"$in": ids}})
			if err != nil {
				return err
			}
			n = res.DeletedCount
			_, err = events.DeleteMany(ctx, M{"_id": M{"$in": ids}})
			return err
		})
		return n, err
	case "J1":
		cur, err := events.Find(ctx, M{"metadane.gatunek": p.Genre, "metadane.tagi": p.Tag}, options.Find().SetProjection(M{"nazwa": 1, "data_rozpoczecia": 1}))
		return count(ctx, cur, err)
	case "J2":
		cur, err := reservations.Aggregate(ctx, mongo.Pipeline{
			{{Key: "$match", Value: M{"utworzono": M{"$gte": p.From, "$lt": p.To}}}},
			{{Key: "$group", Value: M{"_id": "$metadane.kanal", "n": M{"$sum": 1}, "kwota": M{"$sum": "$kwota"}}}},
		})
		return count(ctx, cur, err)
	case "J3":
		res, err := events.UpdateOne(ctx, M{"_id": p.EventID}, M{"$push": M{"metadane.tagi": p.Tag}})
		if err != nil {
			return 0, err
		}
		return res.ModifiedCount, nil
	}
	return 0, fmt.Errorf("mongo: scenario %s not implemented", p.ID)
}

var top10Pipeline = mongo.Pipeline{
	{{Key: "$match", Value: M{"status": model.TicketSold}}},
	{{Key: "$group", Value: M{"_id": "$wydarzenie_id", "sprzedane": M{"$sum": 1}}}},
	{{Key: "$sort", Value: D{{Key: "sprzedane", Value: -1}}}},
	{{Key: "$limit", Value: 10}},
	{{Key: "$lookup", Value: M{"from": "wydarzenia", "localField": "_id", "foreignField": "_id", "as": "wydarzenie"}}},
	{{Key: "$project", Value: M{"sprzedane": 1, "wydarzenie.nazwa": 1}}},
}

func distinct(ctx context.Context, c *mongo.Collection, field string, filter any, out *[]int64) error {
	res := c.Distinct(ctx, field, filter)
	if err := res.Err(); err != nil {
		return err
	}
	return res.Decode(out)
}

func (e *Engine) Prepare(ctx context.Context, p scenario.Params) error {
	f := p.Fixture
	return e.inTx(ctx, func(ctx context.Context) error {
		if len(f.Events) > 0 {
			docs, err := e.fixtureDocs(ctx, f.Events)
			if err != nil {
				return err
			}
			if _, err := insertAll(ctx, e.c("wydarzenia"), docs); err != nil {
				return err
			}
		}
		if _, err := insertAll(ctx, e.c("bilety"), f.Tickets); err != nil {
			return err
		}
		_, err := insertAll(ctx, e.c("rezerwacje"), f.Reservations)
		return err
	})
}

// explainCmd returns the command whose plan represents scenario p.
func explainCmd(p scenario.Params) (D, string) {
	find := func(coll string, filter any, extra ...bson.E) D {
		return append(D{{Key: "find", Value: coll}, {Key: "filter", Value: filter}}, extra...)
	}
	update := func(coll string, filter, upd any, multi bool) D {
		return D{{Key: "update", Value: coll}, {Key: "updates", Value: bson.A{M{"q": filter, "u": upd, "multi": multi}}}}
	}
	del := func(coll string, filter any) D {
		return D{{Key: "delete", Value: coll}, {Key: "deletes", Value: bson.A{M{"q": filter, "limit": 0}}}}
	}
	agg := func(coll string, pipe mongo.Pipeline) D {
		return D{{Key: "aggregate", Value: coll}, {Key: "pipeline", Value: pipe}, {Key: "cursor", Value: M{}}}
	}
	switch p.ID {
	case "C1", "C2":
		return update("bilety", M{"_id": M{"$in": p.TicketIDs}, "status": model.TicketFree}, M{"$set": M{"status": model.TicketReserved}}, true), "bilety.updateMany"
	case "C6":
		return update("rezerwacje", M{"_id": p.ReservationID}, M{"$set": M{"status": model.ReservationPaid}}, false), "rezerwacje.updateOne"
	case "R1", "U1", "D1", "D2":
		return find("rezerwacje", M{"_id": p.ReservationID}), "rezerwacje.find by _id"
	case "R2":
		return find("bilety", M{"wydarzenie_id": p.EventID, "status": model.TicketFree}, bson.E{Key: "sort", Value: D{{Key: "sektor_id", Value: 1}, {Key: "rzad", Value: 1}, {Key: "numer", Value: 1}}}), "bilety.find"
	case "R3":
		return find("wydarzenia", M{"miasto": p.City, "data_rozpoczecia": M{"$gte": p.From, "$lt": p.To}}, bson.E{Key: "sort", Value: M{"data_rozpoczecia": 1}}), "wydarzenia.find"
	case "R4", "D4":
		return find("rezerwacje", M{"klient_id": p.CustomerID}, bson.E{Key: "sort", Value: M{"utworzono": -1}}, bson.E{Key: "limit", Value: 50}), "rezerwacje.find by klient_id"
	case "R5":
		return find("wydarzenia", M{"organizator_id": p.OrganizerID}), "wydarzenia.distinct _id by organizator_id"
	case "R6":
		return agg("bilety", top10Pipeline), "bilety.aggregate top 10"
	case "U2":
		return update("bilety", M{"_id": p.TicketIDs[0], "status": model.TicketFree}, M{"$set": M{"status": model.TicketHeld}}, false), "bilety.updateOne"
	case "U3":
		return update("bilety", M{"wydarzenie_id": p.EventID, "sektor_id": p.SectorID, "status": model.TicketFree}, M{"$mul": M{"cena": p.Factor}}, true), "bilety.updateMany"
	case "U4":
		return update("klienci", M{"_id": p.CustomerID}, M{"$set": M{"email": "x"}}, false), "klienci.updateOne"
	case "U5", "J3":
		return update("wydarzenia", M{"_id": p.EventID}, M{"$set": M{"status": model.EventPlanned}}, false), "wydarzenia.updateOne"
	case "U6":
		return find("rezerwacje", M{"status": model.ReservationPending, "wygasa_o": M{"$lt": p.AsOf}}), "rezerwacje.find expired"
	case "D3":
		return del("rezerwacje", M{"status": model.ReservationExpired, "wygasa_o": M{"$lt": p.Cutoff}}), "rezerwacje.deleteMany"
	case "D5":
		return del("bilety", M{"wydarzenie_id": p.EventID}), "bilety.deleteMany"
	case "D6":
		return del("rezerwacje", M{"utworzono": M{"$lt": p.Cutoff}}), "rezerwacje.deleteMany old"
	case "J1":
		return find("wydarzenia", M{"metadane.gatunek": p.Genre, "metadane.tagi": p.Tag}), "wydarzenia.find by JSON fields"
	case "J2":
		return agg("rezerwacje", mongo.Pipeline{
			{{Key: "$match", Value: M{"utworzono": M{"$gte": p.From, "$lt": p.To}}}},
			{{Key: "$group", Value: M{"_id": "$metadane.kanal", "n": M{"$sum": 1}}}},
		}), "rezerwacje.aggregate by metadane.kanal"
	}
	return nil, ""
}

func (e *Engine) Explain(ctx context.Context, p scenario.Params) (storekit.Plan, error) {
	cmd, label := explainCmd(p)
	if cmd == nil {
		return storekit.Plan{Format: "none", Statement: "insertOne/insertMany", Plan: "inserts have no query plan in MongoDB"}, nil
	}
	var raw bson.Raw
	err := e.db.RunCommand(ctx, D{{Key: "explain", Value: cmd}, {Key: "verbosity", Value: "executionStats"}}).Decode(&raw)
	if err != nil {
		return storekit.Plan{}, err
	}
	js, err := bson.MarshalExtJSONIndent(raw, false, false, "", "  ")
	if err != nil {
		return storekit.Plan{}, err
	}
	stmt, _ := bson.MarshalExtJSON(cmd, false, false)
	return storekit.Plan{Format: "json", Statement: label + " " + string(stmt), Plan: string(js)}, nil
}
