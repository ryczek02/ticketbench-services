// Package enginetest is the conformance suite every storekit.Engine must
// pass. It loads a small dataset, checks read results against an in-memory
// oracle computed from the generator, runs every scenario with and without
// indexes, explains them and runs the contention test for each strategy.
package enginetest

import (
	"context"
	"os"
	"testing"
	"time"

	tbv1 "github.com/ryczek02/ticketbench-contracts/gen/ticketbench/v1"
	"github.com/ryczek02/ticketbench-contracts/pkg/datagen"
	"github.com/ryczek02/ticketbench-contracts/pkg/model"
	"github.com/ryczek02/ticketbench-contracts/pkg/scenario"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
)

// Tickets is the dataset size used by the suite.
const Tickets = 20_000

// SkipWithoutDocker skips container tests in -short mode or when
// TB_SKIP_CONTAINERS is set.
func SkipWithoutDocker(t *testing.T) {
	t.Helper()
	if testing.Short() || os.Getenv("TB_SKIP_CONTAINERS") != "" {
		t.Skip("container test skipped")
	}
}

// Oracle computes expected row counts of read scenarios on freshly loaded
// data, straight from the generator.
func Oracle(d *datagen.Dataset, p scenario.Params) (int64, bool) {
	switch p.ID {
	case "R2":
		for e := range d.Events {
			if d.Events[e].ID == p.EventID {
				var n int64
				for _, t := range d.Sales(e).Tickets {
					if t.Status == model.TicketFree {
						n++
					}
				}
				return n, true
			}
		}
	case "R3":
		var n int64
		for _, ev := range d.Events {
			if d.Venue(&ev).City == p.City && !ev.StartsAt.Before(p.From) && ev.StartsAt.Before(p.To) {
				n++
			}
		}
		return n, true
	case "R4":
		var n int64
		for e := range d.Events {
			for _, r := range d.Sales(e).Reservations {
				if r.CustomerID == p.CustomerID {
					n++
				}
			}
		}
		return min(n, 50), true
	case "R5":
		var n int64
		for _, ev := range d.Events {
			if ev.OrganizerID == p.OrganizerID {
				n++
			}
		}
		return n, true
	case "R6":
		var withSales int64
		for e := range d.Events {
			for _, t := range d.Sales(e).Tickets {
				if t.Status == model.TicketSold {
					withSales++
					break
				}
			}
		}
		return min(withSales, 10), true
	case "J1":
		var n int64
		for _, ev := range d.Events {
			if ev.Meta.Genre != p.Genre {
				continue
			}
			for _, t := range ev.Meta.Tags {
				if t == p.Tag {
					n++
					break
				}
			}
		}
		return n, true
	case "J2":
		groups := map[string]bool{}
		for e := range d.Events {
			for _, r := range d.Sales(e).Reservations {
				if !r.CreatedAt.Before(p.From) && r.CreatedAt.Before(p.To) {
					groups[r.Meta.Channel] = true
				}
			}
		}
		return int64(len(groups)), true
	}
	return 0, false
}

// Run executes the suite against eng.
func Run(t *testing.T, eng storekit.Engine) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	srv := storekit.NewServer(eng)
	spec := &tbv1.DatasetSpec{Tickets: Tickets, Seed: 42}
	d, err := datagen.New(datagen.Spec{Tickets: Tickets, Seed: 42})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := srv.Reset(ctx, &tbv1.ResetRequest{}); err != nil {
		t.Fatalf("reset: %v", err)
	}
	load, err := srv.Load(ctx, &tbv1.LoadRequest{Dataset: spec, Workers: 4, BatchSize: 2000})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if load.Checksum != d.Checksum() {
		t.Fatalf("checksum %s, want %s", load.Checksum, d.Checksum())
	}
	want := d.Counts()
	for _, table := range []string{"bilety", "rezerwacje", "klienci"} {
		if load.Rows[table] != want[table] {
			t.Errorf("loaded %d rows into %s, want %d", load.Rows[table], table, want[table])
		}
	}

	t.Run("oracle", func(t *testing.T) {
		for _, id := range []scenario.ID{"R2", "R3", "R4", "R5", "R6", "J1", "J2"} {
			for it := int64(0); it < 3; it++ {
				p, _ := scenario.For(d, id, it)
				exp, _ := Oracle(d, p)
				res, err := srv.Execute(ctx, &tbv1.ExecuteRequest{Scenario: string(id), Iteration: it, Dataset: spec})
				if err != nil {
					t.Fatalf("%s/%d: %v", id, it, err)
				}
				if res.Rows != exp {
					t.Errorf("%s/%d: %d rows, oracle says %d", id, it, res.Rows, exp)
				}
			}
		}
	})

	runAll := func(t *testing.T, base int64) {
		for _, def := range scenario.All {
			for it := base; it < base+2; it++ {
				res, err := srv.Execute(ctx, &tbv1.ExecuteRequest{Scenario: string(def.ID), Iteration: it, Dataset: spec, TimeoutMs: 60_000})
				if err != nil {
					t.Fatalf("%s/%d: %v", def.ID, it, err)
				}
				if res.TimedOut {
					t.Fatalf("%s/%d timed out", def.ID, it)
				}
				checkRows(t, def.ID, res.Rows)
			}
		}
	}
	t.Run("scenarios without indexes", func(t *testing.T) { runAll(t, 10) })
	if _, err := srv.SetIndexes(ctx, &tbv1.SetIndexesRequest{Enabled: true}); err != nil {
		t.Fatalf("indexes on: %v", err)
	}
	t.Run("scenarios with indexes", func(t *testing.T) { runAll(t, 60) })

	t.Run("explain", func(t *testing.T) {
		for _, def := range scenario.All {
			res, err := srv.Explain(ctx, &tbv1.ExplainRequest{Scenario: string(def.ID), Iteration: 95, Dataset: spec})
			if err != nil {
				t.Fatalf("%s: %v", def.ID, err)
			}
			if res.Format == "" || res.Statement == "" {
				t.Errorf("%s: empty plan", def.ID)
			}
		}
		// D3 after an explain must still see exactly one batch.
		res, err := srv.Execute(ctx, &tbv1.ExecuteRequest{Scenario: "D3", Iteration: 96, Dataset: spec})
		if err != nil {
			t.Fatalf("D3 after explain: %v", err)
		}
		if res.Rows != scenario.BulkReservations {
			t.Errorf("D3 after explain deleted %d rows, want %d", res.Rows, scenario.BulkReservations)
		}
	})

	t.Run("contention", func(t *testing.T) {
		for _, strategy := range eng.Strategies() {
			for _, clients := range []int32{1, 32} {
				res, err := srv.Contend(ctx, &tbv1.ContendRequest{Dataset: spec, Clients: clients, HotTickets: 100, Rounds: 2, Strategy: strategy})
				if err != nil {
					t.Fatalf("%s/%d: %v", strategy, clients, err)
				}
				for _, r := range res.Rounds {
					if !r.Consistent || r.Successes != 100 || r.Errors != 0 {
						t.Errorf("%s/%d round %d: successes=%d errors=%d consistent=%v", strategy, clients, r.Round, r.Successes, r.Errors, r.Consistent)
					}
					if clients == 1 && r.Conflicts != 0 {
						t.Errorf("%s: single client saw %d conflicts", strategy, r.Conflicts)
					}
				}
			}
		}
	})

	t.Run("stats", func(t *testing.T) {
		st, err := srv.Stats(ctx, &tbv1.StatsRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if st.TotalBytes <= 0 || st.Rows["bilety"] <= 0 {
			t.Errorf("stats look empty: %+v", st)
		}
	})

	if _, err := srv.SetIndexes(ctx, &tbv1.SetIndexesRequest{Enabled: false}); err != nil {
		t.Fatalf("indexes off: %v", err)
	}
}

// checkRows asserts row counts that do not depend on the data.
func checkRows(t *testing.T, id scenario.ID, rows int64) {
	t.Helper()
	exact := map[scenario.ID]int64{
		"C3": scenario.BulkTickets,
		"D3": scenario.BulkReservations,
	}
	if want, ok := exact[id]; ok && rows != want {
		t.Errorf("%s: %d rows, want %d", id, rows, want)
	}
	positive := map[scenario.ID]bool{"C1": true, "C2": true, "C4": true, "C5": true, "C6": true, "U2": true, "U5": true, "U6": true, "D5": true, "D6": true, "J3": true}
	if positive[id] && rows <= 0 {
		t.Errorf("%s: no rows affected", id)
	}
}
