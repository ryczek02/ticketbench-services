package valkey

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ryczek02/ticketbench-contracts/pkg/datagen"
	"github.com/ryczek02/ticketbench-contracts/pkg/model"
	"github.com/ryczek02/ticketbench-contracts/pkg/scenario"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
	"github.com/valkey-io/valkey-go"
)

func (e *Engine) ResetHot(ctx context.Context, d *datagen.Dataset, tickets []int64) error {
	e.hotEvent = scenario.HotEventID(d)
	ids, err := e.c.Do(ctx, e.c.B().Smembers().Key("contend:res").Build()).AsStrSlice()
	if err != nil {
		return err
	}
	var cmds valkey.Commands
	for _, id := range ids {
		cmds = append(cmds, e.c.B().Del().Key("r:"+id).Build())
		if on, _ := e.indexesOn(ctx); on {
			cmds = append(cmds, e.c.B().Zrem().Key("ix:r:utw").Member(id).Build(), e.c.B().Zrem().Key("ix:oczek").Member(id).Build())
		}
	}
	cmds = append(cmds, e.c.B().Del().Key("contend:res").Build())
	inv := scenario.SandboxTickets(d, 0)[:len(tickets)]
	cmds = append(cmds, e.salesCmds(inv, nil)...)
	for _, t := range tickets {
		cmds = append(cmds, e.c.B().Del().Key("blok:"+itoa(t)).Build())
	}
	if err := e.multi(ctx, cmds); err != nil {
		return err
	}
	// Indexes of contention reservations live under ix:k:{customer}; drop
	// them so later scenarios do not see ghost ids.
	return e.scan(ctx, "ix:k:*", func(keys []string) error {
		if len(ids) == 0 {
			return nil
		}
		var cmds valkey.Commands
		for _, k := range keys {
			cmds = append(cmds, e.c.B().Zrem().Key(k).Member(ids...).Build())
		}
		return e.multi(ctx, cmds)
	})
}

// VerifyHot reads back the hot tickets; the hot event is the ticket's event.
func (e *Engine) VerifyHot(ctx context.Context, tickets []int64) (int64, int64, error) {
	return e.verify(ctx, e.hotEvent, tickets)
}

func (e *Engine) verify(ctx context.Context, event int64, tickets []int64) (int64, int64, error) {
	args := make([]string, 0, len(tickets)+1)
	args = append(args, itoa(event))
	for _, t := range tickets {
		args = append(args, itoa(t))
	}
	vals, err := verifyScript.Exec(ctx, e.c, nil, args).AsIntSlice()
	if err != nil {
		return 0, 0, err
	}
	return vals[0], vals[1], nil
}

func contentionReservation(b storekit.Booking) model.Reservation {
	exp := b.At.Add(15 * time.Minute)
	return model.Reservation{
		ID: b.ReservationID, CustomerID: b.CustomerID, CreatedAt: b.At, Status: model.ReservationPending,
		ExpiresAt: &exp, Amount: b.Price, Meta: model.ReservationMeta{Channel: "web"},
		Tickets: []model.TicketRef{{TicketID: b.TicketID, EventID: b.EventID, Price: b.Price}},
	}
}

// TryBook strategies:
//
//	lua    one EVALSHA: check the free set, take the seat, write the reservation
//	setnx  SET blok:{ticket} NX PX as a lock (the TTL seat hold), then a
//	       MULTI with SREM, HSET and SET; three round trips
//	watch  optimistic: WATCH wf:{event}, SISMEMBER, MULTI ... EXEC; any change
//	       to the free set aborts the transaction (counted as a conflict)
func (e *Engine) TryBook(ctx context.Context, strategy string, b storekit.Booking) (bool, error) {
	r := contentionReservation(b)
	ev, t := itoa(b.EventID), itoa(b.TicketID)
	switch strategy {
	case "lua":
		n, err := reserveScript.Exec(ctx, e.c, nil, e.reserveArgs(&r, model.TicketReserved)).AsInt64()
		return n == 1, err
	case "setnx":
		err := e.c.Do(ctx, e.c.B().Set().Key("blok:"+t).Value(itoa(b.ReservationID)).Nx().Px(15*time.Minute).Build()).Error()
		if valkey.IsValkeyNil(err) {
			return false, nil // somebody holds the seat
		}
		if err != nil {
			return false, err
		}
		raw, err := e.c.Do(ctx, e.c.B().Hget().Key("wb:"+ev).Field(t).Build()).ToString()
		if err != nil {
			return false, err
		}
		var tk vTicket
		if err := json.Unmarshal([]byte(raw), &tk); err != nil {
			return false, err
		}
		if tk.Status != model.TicketFree {
			return false, nil
		}
		tk.Status, tk.ResID = model.TicketReserved, b.ReservationID
		return true, e.multi(ctx, valkey.Commands{
			e.c.B().Srem().Key("wf:" + ev).Member(t).Build(),
			e.c.B().Hset().Key("wb:"+ev).FieldValue().FieldValue(t, js(tk)).Build(),
			e.c.B().Set().Key("r:" + itoa(r.ID)).Value(js(r)).Build(),
			e.c.B().Sadd().Key("contend:res").Member(itoa(r.ID)).Build(),
		})
	case "watch":
		ok := false
		err := e.c.Dedicated(func(c valkey.DedicatedClient) error {
			if err := c.Do(ctx, c.B().Watch().Key("wf:"+ev).Build()).Error(); err != nil {
				return err
			}
			free, err := c.Do(ctx, c.B().Sismember().Key("wf:"+ev).Member(t).Build()).AsBool()
			if err != nil || !free {
				_ = c.Do(ctx, c.B().Unwatch().Build()).Error()
				return err
			}
			raw, err := c.Do(ctx, c.B().Hget().Key("wb:"+ev).Field(t).Build()).ToString()
			if err != nil {
				return err
			}
			var tk vTicket
			if err := json.Unmarshal([]byte(raw), &tk); err != nil {
				return err
			}
			tk.Status, tk.ResID = model.TicketReserved, b.ReservationID
			res := c.DoMulti(ctx,
				c.B().Multi().Build(),
				c.B().Srem().Key("wf:"+ev).Member(t).Build(),
				c.B().Hset().Key("wb:"+ev).FieldValue().FieldValue(t, js(tk)).Build(),
				c.B().Set().Key("r:"+itoa(r.ID)).Value(js(r)).Build(),
				c.B().Sadd().Key("contend:res").Member(itoa(r.ID)).Build(),
				c.B().Exec().Build(),
			)
			exec := res[len(res)-1]
			if valkey.IsValkeyNil(exec.Error()) {
				return storekit.ErrConflict
			}
			if err := exec.Error(); err != nil {
				return err
			}
			ok = true
			return nil
		})
		return ok, err
	}
	return false, errf("contend", "unknown strategy %s", strategy)
}
