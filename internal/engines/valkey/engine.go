// Package valkey is the Valkey adapter (valkey-go with valkeyotel).
//
// Valkey has no query planner and no secondary indexes, so "indexes on"
// means maintaining extra sets and sorted sets (keys ix:*). With indexes off,
// lookups by attribute fall back to SCAN over the key space. See lua.go for
// the key layout.
package valkey

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ryczek02/ticketbench-contracts/pkg/datagen"
	"github.com/ryczek02/ticketbench-contracts/pkg/model"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeyotel"
)

// Engine implements storekit.Engine.
type Engine struct {
	c valkey.Client
	// hotEvent is remembered by ResetHot for VerifyHot.
	hotEvent int64
}

// Open connects to Valkey at addr (host:port).
func Open(ctx context.Context, addr string, maxConns int) (*Engine, error) {
	c, err := valkeyotel.NewClient(valkey.ClientOption{
		InitAddress:      []string{addr},
		DisableCache:     true,
		BlockingPoolSize: maxConns,
		ConnWriteTimeout: 30 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	if err := c.Do(ctx, c.B().Ping().Build()).Error(); err != nil {
		c.Close()
		return nil, err
	}
	return &Engine{c: c}, nil
}

func (e *Engine) Name() string { return "valkey" }

func (e *Engine) Strategies() []string { return []string{"lua", "setnx", "watch"} }

func (e *Engine) Close() error { e.c.Close(); return nil }

func (e *Engine) ServerVersion(ctx context.Context) (string, error) {
	info, err := e.c.Do(ctx, e.c.B().Info().Section("server").Build()).ToString()
	if err != nil {
		return "", err
	}
	for _, l := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(l), "valkey_version:"); ok {
			return "Valkey " + v, nil
		}
	}
	return "Valkey", nil
}

func (e *Engine) Reset(ctx context.Context) error {
	return e.c.Do(ctx, e.c.B().Flushdb().Build()).Error()
}

func ms(t time.Time) int64 { return t.UnixMilli() }

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

func js(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// vTicket is the compact JSON stored in wb:{event}.
type vTicket struct {
	Seat   int64            `json:"m"`
	Sector int64            `json:"s"`
	Row    int              `json:"r"`
	Number int              `json:"n"`
	Price  float64          `json:"c"`
	Status string           `json:"st"`
	ResID  int64            `json:"rz,omitempty"`
	Held   int64            `json:"zd,omitempty"`
	Meta   model.TicketMeta `json:"md"`
}

func ticketJSON(t *model.Ticket) string {
	v := vTicket{Seat: t.SeatID, Sector: t.SectorID, Row: t.Row, Number: t.Number, Price: t.Price, Status: t.Status, ResID: t.ReservationID, Meta: t.Meta}
	if t.HeldUntil != nil {
		v.Held = ms(*t.HeldUntil)
	}
	return js(v)
}

func (e *Engine) arb(cmd, key string, args ...string) valkey.Completed {
	return e.c.B().Arbitrary(cmd).Keys(key).Args(args...).Build()
}

func (e *Engine) multi(ctx context.Context, cmds valkey.Commands) error {
	if len(cmds) == 0 {
		return nil
	}
	for _, r := range e.c.DoMulti(ctx, cmds...) {
		if err := r.Error(); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) eventCmds(d venueLookup, evs []model.Event) valkey.Commands {
	var cmds valkey.Commands
	ids := make([]string, 0, len(evs))
	for i := range evs {
		ev := &evs[i]
		city := d.city(ev.VenueID)
		cmds = append(cmds, e.arb("HSET", "w:"+itoa(ev.ID),
			"nazwa", ev.Name, "organizator_id", itoa(ev.OrganizerID), "kategoria_id", itoa(ev.CategoryID),
			"obiekt_id", itoa(ev.VenueID), "miasto", city, "data", itoa(ms(ev.StartsAt)), "status", ev.Status,
			"gatunek", ev.Meta.Genre, "metadane", js(ev.Meta), "cennik", js(ev.Prices)))
		ids = append(ids, itoa(ev.ID))
	}
	if len(ids) > 0 {
		cmds = append(cmds, e.arb("SADD", "w:all", ids...))
	}
	return cmds
}

// salesCmds writes tickets (inventory and free set), reservations and the
// sales counters of one batch.
func (e *Engine) salesCmds(tickets []model.Ticket, reservations []model.Reservation) valkey.Commands {
	var cmds valkey.Commands
	byEvent := map[int64][]string{}
	free := map[int64][]string{}
	sold := map[int64]int64{}
	revenue := map[int64]float64{}
	for i := range tickets {
		t := &tickets[i]
		byEvent[t.EventID] = append(byEvent[t.EventID], itoa(t.ID), ticketJSON(t))
		switch t.Status {
		case model.TicketFree:
			free[t.EventID] = append(free[t.EventID], itoa(t.ID))
		case model.TicketSold:
			sold[t.EventID]++
			revenue[t.EventID] += t.Price
		}
	}
	for ev, fv := range byEvent {
		_ = storekit.Chunks(len(fv)/2, 1000, func(from, to int) error {
			cmds = append(cmds, e.arb("HSET", "wb:"+itoa(ev), fv[from*2:to*2]...))
			return nil
		})
	}
	for ev, ids := range free {
		cmds = append(cmds, e.arb("SADD", "wf:"+itoa(ev), ids...))
	}
	for ev, n := range sold {
		cmds = append(cmds, e.c.B().Zincrby().Key("sprzedaz").Increment(float64(n)).Member(itoa(ev)).Build())
		cmds = append(cmds, e.c.B().Hincrbyfloat().Key("przychod").Field(itoa(ev)).Increment(revenue[ev]).Build())
	}
	for i := range reservations {
		r := &reservations[i]
		cmds = append(cmds, e.c.B().Set().Key("r:"+itoa(r.ID)).Value(js(r)).Build())
	}
	return cmds
}

// resIndexCmds maintains the reservation indexes for r.
func (e *Engine) resIndexCmds(r *model.Reservation) valkey.Commands {
	id := itoa(r.ID)
	cmds := valkey.Commands{
		e.c.B().Zadd().Key("ix:k:"+itoa(r.CustomerID)).ScoreMember().ScoreMember(float64(ms(r.CreatedAt)), id).Build(),
		e.c.B().Zadd().Key("ix:r:utw").ScoreMember().ScoreMember(float64(ms(r.CreatedAt)), id).Build(),
	}
	if r.ExpiresAt != nil {
		switch r.Status {
		case model.ReservationPending:
			cmds = append(cmds, e.c.B().Zadd().Key("ix:oczek").ScoreMember().ScoreMember(float64(ms(*r.ExpiresAt)), id).Build())
		case model.ReservationExpired:
			cmds = append(cmds, e.c.B().Zadd().Key("ix:wygasle").ScoreMember().ScoreMember(float64(ms(*r.ExpiresAt)), id).Build())
		}
	}
	return cmds
}

type eventHead struct {
	id                    int64
	city, genre, status   string
	organizer, startsAtMs int64
}

func (e *Engine) eventIndexCmds(h eventHead) valkey.Commands {
	id := itoa(h.id)
	return valkey.Commands{
		e.c.B().Zadd().Key("ix:miasto:"+h.city).ScoreMember().ScoreMember(float64(h.startsAtMs), id).Build(),
		e.c.B().Sadd().Key("ix:org:" + itoa(h.organizer)).Member(id).Build(),
		e.c.B().Sadd().Key("ix:gatunek:" + h.genre).Member(id).Build(),
		e.c.B().Zadd().Key("ix:w:data").ScoreMember().ScoreMember(float64(h.startsAtMs), id).Build(),
	}
}

func (e *Engine) indexesOn(ctx context.Context) (bool, error) {
	n, err := e.c.Do(ctx, e.c.B().Exists().Key("ix:on").Build()).AsInt64()
	return n == 1, err
}

type venueLookup interface{ city(id int64) string }

type datasetVenues struct{ d *datagen.Dataset }

func (v datasetVenues) city(id int64) string { return v.d.Venues[id-1].City }

type dbVenues struct {
	ctx context.Context
	e   *Engine
}

func (v dbVenues) city(id int64) string {
	c, _ := v.e.c.Do(v.ctx, v.e.c.B().Hget().Key("obj:"+itoa(id)).Field("miasto").Build()).ToString()
	return c
}

func (e *Engine) Load(ctx context.Context, d *datagen.Dataset, opts storekit.LoadOptions) (map[string]int64, error) {
	rows := map[string]int64{}
	var cmds valkey.Commands
	for _, c := range d.Categories {
		cmds = append(cmds, e.arb("HSET", "kat:"+itoa(c.ID), "nazwa", c.Name))
	}
	for _, o := range d.Organizers {
		cmds = append(cmds, e.arb("HSET", "org:"+itoa(o.ID), "nazwa", o.Name, "nip", o.TaxID, "email", o.Email))
	}
	for _, v := range d.Venues {
		cmds = append(cmds, e.arb("HSET", "obj:"+itoa(v.ID), "nazwa", v.Name, "miasto", v.City, "adres", v.Address,
			"pojemnosc", strconv.Itoa(v.Capacity), "metadane", js(v.Meta), "sektory", js(v.Sectors)))
	}
	for _, k := range d.Discounts {
		cmds = append(cmds, e.arb("HSET", "kod:"+itoa(k.ID), "kod", k.Code, "procent", strconv.Itoa(k.Percent),
			"wazny_od", itoa(ms(k.ValidFrom)), "wazny_do", itoa(ms(k.ValidUntil)), "limit_uzyc", strconv.Itoa(k.UsageLimit)))
	}
	cmds = append(cmds, e.eventCmds(datasetVenues{d}, d.Events)...)
	if err := e.multi(ctx, cmds); err != nil {
		return nil, err
	}
	rows["kategorie"], rows["organizatorzy"], rows["obiekty"] = int64(len(d.Categories)), int64(len(d.Organizers)), int64(len(d.Venues))
	rows["kody_rabatowe"], rows["wydarzenia"] = int64(len(d.Discounts)), int64(len(d.Events))

	if err := storekit.ParallelCustomers(ctx, d, opts.Workers, 5000, func(ctx context.Context, cs []model.Customer) error {
		cmds := make(valkey.Commands, 0, len(cs))
		for i := range cs {
			cmds = append(cmds, e.customerCmd(&cs[i]))
		}
		return e.multi(ctx, cmds)
	}); err != nil {
		return nil, err
	}
	rows["klienci"] = d.Customers
	n, err := storekit.ParallelSales(ctx, d, opts.Workers, opts.BatchSize, func(ctx context.Context, b *storekit.SalesBatch) error {
		return e.multi(ctx, e.salesCmds(b.Tickets, b.Reservations))
	})
	if err != nil {
		return nil, err
	}
	rows["bilety"], rows["rezerwacje"], rows["platnosci"] = n.Tickets, n.Reservations, n.Payments
	return rows, nil
}

func (e *Engine) customerCmd(c *model.Customer) valkey.Completed {
	return e.arb("HSET", "k:"+itoa(c.ID), "imie", c.FirstName, "nazwisko", c.LastName, "email", c.Email,
		"telefon", c.Phone, "miasto", c.City, "utworzono", itoa(ms(c.CreatedAt)), "preferencje", js(c.Prefs))
}

// scan calls fn with batches of keys matching pattern.
func (e *Engine) scan(ctx context.Context, pattern string, fn func([]string) error) error {
	var cursor uint64
	for {
		entry, err := e.c.Do(ctx, e.c.B().Scan().Cursor(cursor).Match(pattern).Count(2000).Build()).AsScanEntry()
		if err != nil {
			return err
		}
		if len(entry.Elements) > 0 {
			if err := fn(entry.Elements); err != nil {
				return err
			}
		}
		if entry.Cursor == 0 {
			return nil
		}
		cursor = entry.Cursor
	}
}

// scanReservations decodes every reservation (index-free fallback).
func (e *Engine) scanReservations(ctx context.Context, keep func(*model.Reservation) bool) ([]model.Reservation, error) {
	var out []model.Reservation
	err := e.scan(ctx, "r:*", func(keys []string) error {
		vals, err := e.c.Do(ctx, e.c.B().Mget().Key(keys...).Build()).AsStrSlice()
		if err != nil {
			return err
		}
		for _, v := range vals {
			if v == "" {
				continue
			}
			var r model.Reservation
			if err := json.Unmarshal([]byte(v), &r); err != nil {
				return err
			}
			if keep(&r) {
				out = append(out, r)
			}
		}
		return ctx.Err()
	})
	return out, err
}

func (e *Engine) eventIDs(ctx context.Context) ([]string, error) {
	return e.c.Do(ctx, e.c.B().Smembers().Key("w:all").Build()).AsStrSlice()
}

// eventHeads loads the indexed attributes of the given events.
func (e *Engine) eventHeads(ctx context.Context, ids []string) ([]eventHead, error) {
	cmds := make(valkey.Commands, len(ids))
	for i, id := range ids {
		cmds[i] = e.c.B().Hmget().Key("w:"+id).Field("miasto", "gatunek", "status", "organizator_id", "data").Build()
	}
	var out []eventHead
	for i, r := range e.c.DoMulti(ctx, cmds...) {
		f, err := r.AsStrSlice()
		if err != nil {
			return nil, err
		}
		if f[0] == "" {
			continue
		}
		id, _ := strconv.ParseInt(ids[i], 10, 64)
		org, _ := strconv.ParseInt(f[3], 10, 64)
		at, _ := strconv.ParseInt(f[4], 10, 64)
		out = append(out, eventHead{id: id, city: f[0], genre: f[1], status: f[2], organizer: org, startsAtMs: at})
	}
	return out, nil
}

func (e *Engine) SetIndexes(ctx context.Context, enabled bool) error {
	if !enabled {
		if err := e.c.Do(ctx, e.c.B().Del().Key("ix:on").Build()).Error(); err != nil {
			return err
		}
		return e.scan(ctx, "ix:*", func(keys []string) error {
			return e.c.Do(ctx, e.c.B().Unlink().Key(keys...).Build()).Error()
		})
	}
	ids, err := e.eventIDs(ctx)
	if err != nil {
		return err
	}
	heads, err := e.eventHeads(ctx, ids)
	if err != nil {
		return err
	}
	var cmds valkey.Commands
	for _, h := range heads {
		cmds = append(cmds, e.eventIndexCmds(h)...)
	}
	if err := e.multi(ctx, cmds); err != nil {
		return err
	}
	cmds = cmds[:0]
	_, err = e.scanReservations(ctx, func(r *model.Reservation) bool {
		cmds = append(cmds, e.resIndexCmds(r)...)
		return false
	})
	if err != nil {
		return err
	}
	if err := storekit.Chunks(len(cmds), 10000, func(from, to int) error { return e.multi(ctx, cmds[from:to]) }); err != nil {
		return err
	}
	return e.c.Do(ctx, e.c.B().Set().Key("ix:on").Value("1").Build()).Error()
}

func (e *Engine) Stats(ctx context.Context) (storekit.Stats, error) {
	st := storekit.Stats{Rows: map[string]int64{}}
	info, err := e.c.Do(ctx, e.c.B().Info().Section("memory").Build()).ToString()
	if err != nil {
		return st, err
	}
	field := func(name string) int64 {
		for _, l := range strings.Split(info, "\n") {
			if v, ok := strings.CutPrefix(strings.TrimSpace(l), name+":"); ok {
				n, _ := strconv.ParseInt(v, 10, 64)
				return n
			}
		}
		return 0
	}
	st.TotalBytes = field("used_memory")
	st.DataBytes = field("used_memory_dataset")
	var ixKeys int64
	if err := e.scan(ctx, "ix:*", func(k []string) error { ixKeys += int64(len(k)); return nil }); err != nil {
		return st, err
	}
	st.IndexBytes = 0 // Valkey does not report memory per key group cheaply
	ids, err := e.eventIDs(ctx)
	if err != nil {
		return st, err
	}
	cmds := make(valkey.Commands, len(ids))
	for i, id := range ids {
		cmds[i] = e.c.B().Hlen().Key("wb:" + id).Build()
	}
	var tickets int64
	for _, r := range e.c.DoMulti(ctx, cmds...) {
		n, _ := r.AsInt64()
		tickets += n
	}
	count := func(pattern string) (int64, error) {
		var n int64
		err := e.scan(ctx, pattern, func(k []string) error { n += int64(len(k)); return nil })
		return n, err
	}
	st.Rows["wydarzenia"] = int64(len(ids))
	st.Rows["bilety"] = tickets
	if st.Rows["rezerwacje"], err = count("r:*"); err != nil {
		return st, err
	}
	if st.Rows["klienci"], err = count("k:*"); err != nil {
		return st, err
	}
	st.Rows["ix_keys"] = ixKeys
	return st, nil
}

func errf(id string, format string, args ...any) error {
	return fmt.Errorf("valkey %s: "+format, append([]any{id}, args...)...)
}
