package valkey

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"

	"github.com/ryczek02/ticketbench-contracts/pkg/model"
	"github.com/ryczek02/ticketbench-contracts/pkg/scenario"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
	"github.com/valkey-io/valkey-go"
)

func (e *Engine) reserveArgs(r *model.Reservation, ticketStatus string) []string {
	exp := ""
	if r.ExpiresAt != nil {
		exp = itoa(ms(*r.ExpiresAt))
	}
	args := []string{itoa(r.ID), js(r), ticketStatus, itoa(r.CustomerID), itoa(ms(r.CreatedAt)), exp, strconv.Itoa(len(r.Tickets))}
	for _, t := range r.Tickets {
		args = append(args, itoa(t.EventID), itoa(t.TicketID))
	}
	return args
}

// reservationsBy returns reservations matching keep, through the index zset
// when indexes are on (ids between min and max score), else with SCAN.
func (e *Engine) reservationsBy(ctx context.Context, on bool, zset, minS, maxS string, rev bool, limit int64, keep func(*model.Reservation) bool) ([]model.Reservation, error) {
	if !on {
		return e.scanReservations(ctx, keep)
	}
	var cmd valkey.Completed
	if rev {
		b := e.c.B().Zrange().Key(zset).Min(maxS).Max(minS).Byscore().Rev()
		if limit > 0 {
			cmd = b.Limit(0, limit).Build()
		} else {
			cmd = b.Build()
		}
	} else {
		cmd = e.c.B().Zrange().Key(zset).Min(minS).Max(maxS).Byscore().Build()
	}
	ids, err := e.c.Do(ctx, cmd).AsStrSlice()
	if err != nil || len(ids) == 0 {
		return nil, err
	}
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = "r:" + id
	}
	var out []model.Reservation
	err = storekit.Chunks(len(keys), 1000, func(from, to int) error {
		vals, err := e.c.Do(ctx, e.c.B().Mget().Key(keys[from:to]...).Build()).AsStrSlice()
		if err != nil {
			return err
		}
		for _, v := range vals {
			var r model.Reservation
			if v != "" && json.Unmarshal([]byte(v), &r) == nil && keep(&r) {
				out = append(out, r)
			}
		}
		return nil
	})
	return out, err
}

func (e *Engine) eventsBy(ctx context.Context, on bool, indexed func() ([]string, error), keep func(eventHead) bool) ([]eventHead, error) {
	var ids []string
	var err error
	if on {
		ids, err = indexed()
	} else {
		ids, err = e.eventIDs(ctx)
	}
	if err != nil {
		return nil, err
	}
	heads, err := e.eventHeads(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := heads[:0]
	for _, h := range heads {
		if keep(h) {
			out = append(out, h)
		}
	}
	return out, nil
}

func (e *Engine) releaseAll(ctx context.Context, mode string, rs []model.Reservation) (tickets, reservations int64, err error) {
	if len(rs) == 0 {
		return 0, 0, nil
	}
	execs := make([]valkey.LuaExec, len(rs))
	for i := range rs {
		exp := "0"
		if rs[i].ExpiresAt != nil {
			exp = itoa(ms(*rs[i].ExpiresAt))
		}
		execs[i] = valkey.LuaExec{Args: []string{itoa(rs[i].ID), mode, exp}}
	}
	for _, r := range releaseScript.ExecMulti(ctx, e.c, execs...) {
		n, err := r.AsInt64()
		if err != nil {
			return 0, 0, err
		}
		if n >= 0 {
			tickets += n
			reservations++
		}
	}
	return tickets, reservations, nil
}

func (e *Engine) Run(ctx context.Context, p scenario.Params) (int64, error) {
	on, err := e.indexesOn(ctx)
	if err != nil {
		return 0, err
	}
	id := string(p.ID)
	switch p.ID {
	case "C1", "C2":
		n, err := reserveScript.Exec(ctx, e.c, nil, e.reserveArgs(p.Reservation, model.TicketReserved)).AsInt64()
		if err == nil && n != int64(len(p.TicketIDs)) {
			err = errf(id, "reserved %d of %d tickets", n, len(p.TicketIDs))
		}
		return n, err
	case "C3":
		err := storekit.Chunks(len(p.Tickets), 2000, func(from, to int) error {
			return e.multi(ctx, e.salesCmds(p.Tickets[from:to], nil))
		})
		return int64(len(p.Tickets)), err
	case "C4":
		return 1, e.c.Do(ctx, e.customerCmd(p.Customer)).Error()
	case "C5":
		cmds := e.eventCmds(dbVenues{ctx, e}, []model.Event{*p.Event})
		if on {
			cmds = append(cmds, e.eventIndexCmds(eventHead{id: p.Event.ID, city: dbVenues{ctx, e}.city(p.Event.VenueID), genre: p.Event.Meta.Genre,
				organizer: p.Event.OrganizerID, startsAtMs: ms(p.Event.StartsAt)})...)
		}
		return 1, e.multi(ctx, cmds)
	case "C6":
		return statusScript.Exec(ctx, e.c, nil, []string{itoa(p.ReservationID), model.ReservationPaid, js(p.Payment)}).AsInt64()
	case "R1":
		v, err := e.c.Do(ctx, e.c.B().Get().Key("r:"+itoa(p.ReservationID)).Build()).ToString()
		if valkey.IsValkeyNil(err) {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		var r model.Reservation
		if err := json.Unmarshal([]byte(v), &r); err != nil {
			return 0, err
		}
		return int64(max(1, len(r.Tickets))), nil
	case "R2":
		ev := itoa(p.EventID)
		free, err := e.c.Do(ctx, e.c.B().Smembers().Key("wf:"+ev).Build()).AsStrSlice()
		if err != nil || len(free) == 0 {
			return 0, err
		}
		vals, err := e.c.Do(ctx, e.c.B().Hmget().Key("wb:"+ev).Field(free...).Build()).AsStrSlice()
		if err != nil {
			return 0, err
		}
		seats := make([]vTicket, 0, len(vals))
		for _, v := range vals {
			var t vTicket
			if err := json.Unmarshal([]byte(v), &t); err != nil {
				return 0, err
			}
			seats = append(seats, t)
		}
		slices.SortFunc(seats, func(a, b vTicket) int {
			if a.Sector != b.Sector {
				return int(a.Sector - b.Sector)
			}
			if a.Row != b.Row {
				return a.Row - b.Row
			}
			return a.Number - b.Number
		})
		return int64(len(seats)), nil
	case "R3":
		from, to := ms(p.From), ms(p.To)
		hs, err := e.eventsBy(ctx, on, func() ([]string, error) {
			return e.c.Do(ctx, e.c.B().Zrange().Key("ix:miasto:"+p.City).Min(itoa(from)).Max("("+itoa(to)).Byscore().Build()).AsStrSlice()
		}, func(h eventHead) bool { return h.city == p.City && h.startsAtMs >= from && h.startsAtMs < to })
		if err != nil {
			return 0, err
		}
		return e.fetchEvents(ctx, hs, "nazwa", "data", "obiekt_id", "kategoria_id")
	case "R4":
		rs, err := e.reservationsBy(ctx, on, "ix:k:"+itoa(p.CustomerID), "-inf", "+inf", true, 50,
			func(r *model.Reservation) bool { return r.CustomerID == p.CustomerID })
		if err != nil {
			return 0, err
		}
		slices.SortFunc(rs, func(a, b model.Reservation) int { return b.CreatedAt.Compare(a.CreatedAt) })
		return int64(min(len(rs), 50)), nil
	case "R5":
		hs, err := e.eventsBy(ctx, on, func() ([]string, error) {
			return e.c.Do(ctx, e.c.B().Smembers().Key("ix:org:"+itoa(p.OrganizerID)).Build()).AsStrSlice()
		}, func(h eventHead) bool { return h.organizer == p.OrganizerID })
		if err != nil || len(hs) == 0 {
			return 0, err
		}
		var cmds valkey.Commands
		for _, h := range hs {
			cmds = append(cmds, e.c.B().Zscore().Key("sprzedaz").Member(itoa(h.id)).Build(),
				e.c.B().Hget().Key("przychod").Field(itoa(h.id)).Build())
		}
		for _, r := range e.c.DoMulti(ctx, cmds...) {
			if err := r.Error(); err != nil && !valkey.IsValkeyNil(err) {
				return 0, err
			}
		}
		return int64(len(hs)), nil
	case "R6":
		top, err := e.c.Do(ctx, e.c.B().Zrange().Key("sprzedaz").Min("+inf").Max("1").Byscore().Rev().Limit(0, 10).Withscores().Build()).AsZScores()
		if err != nil || len(top) == 0 {
			return 0, err
		}
		cmds := make(valkey.Commands, len(top))
		for i, z := range top {
			cmds[i] = e.c.B().Hget().Key("w:" + z.Member).Field("nazwa").Build()
		}
		return int64(len(top)), e.multi(ctx, cmds)
	case "U1":
		return statusScript.Exec(ctx, e.c, nil, []string{itoa(p.ReservationID), model.ReservationPaid, ""}).AsInt64()
	case "U2":
		n, err := holdScript.Exec(ctx, e.c, nil, []string{itoa(p.EventID), itoa(p.TicketIDs[0]), itoa(ms(p.NewDate)), "600000"}).AsInt64()
		if err == nil && n != 1 {
			err = errf(id, "ticket %d not free", p.TicketIDs[0])
		}
		return n, err
	case "U3":
		return sectorPriceScript.Exec(ctx, e.c, nil, []string{itoa(p.EventID), itoa(p.SectorID), strconv.FormatFloat(p.Factor, 'f', -1, 64)}).AsInt64()
	case "U4":
		c := p.Customer
		key := "k:" + itoa(c.ID)
		cmds := valkey.Commands{
			e.c.B().Hset().Key(key).FieldValue().FieldValue("email", c.Email).FieldValue("telefon", c.Phone).Build(),
		}
		if err := e.multi(ctx, cmds); err != nil {
			return 0, err
		}
		return hjsonScript.Exec(ctx, e.c, []string{key}, []string{"preferencje", "merge", js(map[string]any{"jezyk": c.Prefs.Language, "newsletter": c.Prefs.Newsletter})}).AsInt64()
	case "U5":
		key := "w:" + itoa(p.EventID)
		f, err := e.c.Do(ctx, e.c.B().Hmget().Key(key).Field("miasto").Build()).AsStrSlice()
		if err != nil {
			return 0, err
		}
		at := ms(p.NewDate)
		cmds := valkey.Commands{e.c.B().Hset().Key(key).FieldValue().FieldValue("data", itoa(at)).Build()}
		if on {
			cmds = append(cmds,
				e.c.B().Zadd().Key("ix:miasto:"+f[0]).ScoreMember().ScoreMember(float64(at), itoa(p.EventID)).Build(),
				e.c.B().Zadd().Key("ix:w:data").ScoreMember().ScoreMember(float64(at), itoa(p.EventID)).Build())
		}
		return 1, e.multi(ctx, cmds)
	case "U6":
		asOf := p.AsOf
		rs, err := e.reservationsBy(ctx, on, "ix:oczek", "-inf", "("+itoa(ms(asOf)), false, 0, func(r *model.Reservation) bool {
			return r.Status == model.ReservationPending && r.ExpiresAt != nil && r.ExpiresAt.Before(asOf)
		})
		if err != nil {
			return 0, err
		}
		n, _, err := e.releaseAll(ctx, "expire", rs)
		if err == nil && n != scenario.BulkReservations {
			err = errf(id, "released %d tickets, want %d", n, scenario.BulkReservations)
		}
		return n, err
	case "D1", "D2":
		mode := "cancel"
		if p.ID == "D2" {
			mode = "drop"
		}
		n, err := releaseScript.Exec(ctx, e.c, nil, []string{itoa(p.ReservationID), mode, "0"}).AsInt64()
		if err != nil || n < 0 {
			return 0, err
		}
		return 1, nil
	case "D3":
		cutoff := p.Cutoff
		rs, err := e.reservationsBy(ctx, on, "ix:wygasle", "-inf", "("+itoa(ms(cutoff)), false, 0, func(r *model.Reservation) bool {
			return r.Status == model.ReservationExpired && r.ExpiresAt != nil && r.ExpiresAt.Before(cutoff)
		})
		if err != nil {
			return 0, err
		}
		_, n, err := e.releaseAll(ctx, "drop", rs)
		return n, err
	case "D4":
		rs, err := e.reservationsBy(ctx, on, "ix:k:"+itoa(p.CustomerID), "-inf", "+inf", false, 0,
			func(r *model.Reservation) bool { return r.CustomerID == p.CustomerID })
		if err != nil {
			return 0, err
		}
		_, n, err := e.releaseAll(ctx, "cancel", rs)
		if err != nil {
			return 0, err
		}
		return n, e.c.Do(ctx, e.c.B().Del().Key("k:"+itoa(p.CustomerID), "ix:k:"+itoa(p.CustomerID)).Build()).Error()
	case "D5":
		return dropEventScript.Exec(ctx, e.c, nil, []string{itoa(p.EventID), model.EventCancelled}).AsInt64()
	case "D6":
		cutoff := ms(p.Cutoff)
		rs, err := e.reservationsBy(ctx, on, "ix:r:utw", "-inf", "("+itoa(cutoff), false, 0,
			func(r *model.Reservation) bool { return ms(r.CreatedAt) < cutoff })
		if err != nil {
			return 0, err
		}
		if _, _, err := e.releaseAll(ctx, "drop", rs); err != nil {
			return 0, err
		}
		hs, err := e.eventsBy(ctx, on, func() ([]string, error) {
			return e.c.Do(ctx, e.c.B().Zrange().Key("ix:w:data").Min("-inf").Max("("+itoa(cutoff)).Byscore().Build()).AsStrSlice()
		}, func(h eventHead) bool { return h.startsAtMs < cutoff })
		if err != nil || len(hs) == 0 {
			return 0, err
		}
		execs := make([]valkey.LuaExec, len(hs))
		for i, h := range hs {
			execs[i] = valkey.LuaExec{Args: []string{itoa(h.id), ""}}
		}
		// Tickets of old events that belonged to the deleted reservations
		// are already gone ("drop"); count what the event delete removes
		// plus those.
		var n int64
		for _, r := range rs {
			n += int64(len(r.Tickets))
		}
		for _, r := range dropEventScript.ExecMulti(ctx, e.c, execs...) {
			k, err := r.AsInt64()
			if err != nil {
				return 0, err
			}
			n += max(k, 0)
		}
		return n, nil
	case "J1":
		hs, err := e.eventsBy(ctx, on, func() ([]string, error) {
			return e.c.Do(ctx, e.c.B().Smembers().Key("ix:gatunek:"+p.Genre).Build()).AsStrSlice()
		}, func(h eventHead) bool { return h.genre == p.Genre })
		if err != nil || len(hs) == 0 {
			return 0, err
		}
		cmds := make(valkey.Commands, len(hs))
		for i, h := range hs {
			cmds[i] = e.c.B().Hget().Key("w:" + itoa(h.id)).Field("metadane").Build()
		}
		var n int64
		for _, r := range e.c.DoMulti(ctx, cmds...) {
			v, err := r.ToString()
			if err != nil {
				return 0, err
			}
			var m model.EventMeta
			if err := json.Unmarshal([]byte(v), &m); err != nil {
				return 0, err
			}
			if slices.Contains(m.Tags, p.Tag) {
				n++
			}
		}
		return n, nil
	case "J2":
		from, to := p.From, p.To
		rs, err := e.reservationsBy(ctx, on, "ix:r:utw", itoa(ms(from)), "("+itoa(ms(to)), false, 0, func(r *model.Reservation) bool {
			return !r.CreatedAt.Before(from) && r.CreatedAt.Before(to)
		})
		if err != nil {
			return 0, err
		}
		groups := map[string]float64{}
		for _, r := range rs {
			groups[r.Meta.Channel] += r.Amount
		}
		return int64(len(groups)), nil
	case "J3":
		return hjsonScript.Exec(ctx, e.c, []string{"w:" + itoa(p.EventID)}, []string{"metadane", "push", "tagi", p.Tag}).AsInt64()
	}
	return 0, errf(id, "not implemented")
}

func (e *Engine) fetchEvents(ctx context.Context, hs []eventHead, fields ...string) (int64, error) {
	if len(hs) == 0 {
		return 0, nil
	}
	cmds := make(valkey.Commands, len(hs))
	for i, h := range hs {
		cmds[i] = e.c.B().Hmget().Key("w:" + itoa(h.id)).Field(fields...).Build()
	}
	return int64(len(hs)), e.multi(ctx, cmds)
}

func (e *Engine) Prepare(ctx context.Context, p scenario.Params) error {
	f := p.Fixture
	on, err := e.indexesOn(ctx)
	if err != nil {
		return err
	}
	cmds := e.eventCmds(dbVenues{ctx, e}, f.Events)
	if on {
		for _, ev := range f.Events {
			cmds = append(cmds, e.eventIndexCmds(eventHead{id: ev.ID, city: dbVenues{ctx, e}.city(ev.VenueID), genre: ev.Meta.Genre,
				organizer: ev.OrganizerID, startsAtMs: ms(ev.StartsAt)})...)
		}
	}
	cmds = append(cmds, e.salesCmds(f.Tickets, f.Reservations)...)
	if on {
		for i := range f.Reservations {
			cmds = append(cmds, e.resIndexCmds(&f.Reservations[i])...)
		}
	}
	return e.multi(ctx, cmds)
}

// Explain describes the commands a scenario issues; Valkey has no planner.
func (e *Engine) Explain(ctx context.Context, p scenario.Params) (storekit.Plan, error) {
	on, err := e.indexesOn(ctx)
	if err != nil {
		return storekit.Plan{}, err
	}
	lookup := map[scenario.ID][2]string{
		"C1": {"EVALSHA reserve", "SISMEMBER wf:{e} + SREM + HSET wb:{e} + SET r:{id}, O(tickets)"},
		"C2": {"EVALSHA reserve", "SISMEMBER wf:{e} + SREM + HSET wb:{e} + SET r:{id}, O(tickets)"},
		"C3": {"HSET wb:{e} ... + SADD wf:{e} ...", "pipelined, O(tickets)"},
		"C4": {"HSET k:{id}", "O(fields)"},
		"C5": {"HSET w:{id} + SADD w:all", "O(1)"},
		"C6": {"EVALSHA setstatus", "GET + SET r:{id}, O(size of JSON)"},
		"R1": {"GET r:{id}", "O(1) key lookup"},
		"R2": {"SMEMBERS wf:{e} + HMGET wb:{e}", "O(free seats), sorted in the client"},
		"R5": {"SMEMBERS ix:org:{o} | SMEMBERS w:all + HMGET, then ZSCORE sprzedaz", "counters kept on write"},
		"R6": {"ZRANGE sprzedaz +inf 1 BYSCORE REV LIMIT 0 10", "O(log N + 10), precomputed counter"},
		"U1": {"EVALSHA setstatus", "O(size of JSON)"},
		"U2": {"EVALSHA hold", "SREM wf:{e} + HSET wb:{e} + SET blok:{t} PX"},
		"U3": {"EVALSHA sectorprice", "HGETALL wb:{e}, O(tickets of event)"},
		"U4": {"HSET k:{id} + EVALSHA hjson merge", "O(1)"},
		"U5": {"HSET w:{id} data (+ ZADD ix:*)", "O(log N)"},
		"D1": {"EVALSHA release cancel", "O(tickets of reservation)"},
		"D2": {"EVALSHA release drop", "O(tickets of reservation)"},
		"D5": {"EVALSHA dropevent", "DEL wb:{e} wf:{e} w:{e}, O(tickets)"},
		"J3": {"EVALSHA hjson push", "O(size of JSON)"},
	}
	scan := map[scenario.ID][2]string{
		"R3": {"ZRANGE ix:miasto:{c} BYSCORE", "SMEMBERS w:all + HMGET every event"},
		"R4": {"ZRANGE ix:k:{id} BYSCORE REV LIMIT 0 50 + MGET", "SCAN r:* + MGET + JSON decode of every reservation"},
		"U6": {"ZRANGE ix:oczek -inf asOf BYSCORE + EVALSHA release", "SCAN r:* + MGET + filter, then EVALSHA release"},
		"D3": {"ZRANGE ix:wygasle BYSCORE + EVALSHA release", "SCAN r:* + filter + EVALSHA release"},
		"D4": {"ZRANGE ix:k:{id} + EVALSHA release", "SCAN r:* + filter + EVALSHA release"},
		"D6": {"ZRANGE ix:r:utw + ZRANGE ix:w:data + EVALSHA", "SCAN r:* + SMEMBERS w:all + filter"},
		"J1": {"SMEMBERS ix:gatunek:{g} + HGET metadane", "SMEMBERS w:all + HGET metadane of every event"},
		"J2": {"ZRANGE ix:r:utw BYSCORE + MGET", "SCAN r:* + MGET + JSON decode"},
	}
	if v, ok := lookup[p.ID]; ok {
		return storekit.Plan{Format: "none", Statement: v[0], Plan: v[1]}, nil
	}
	v := scan[p.ID]
	plan := v[1]
	if on {
		plan = v[0]
	}
	return storekit.Plan{Format: "none", Statement: strings.TrimSpace(v[0] + " | " + v[1]), Plan: plan}, nil
}
