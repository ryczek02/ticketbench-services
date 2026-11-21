package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/ryczek02/ticketbench-contracts/pkg/model"
	"github.com/ryczek02/ticketbench-contracts/pkg/scenario"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
)

type stmt struct {
	sql   string
	args  []any
	query bool
}

// plan is the list of statements of one scenario. main points at the
// statement whose query plan is reported by Explain and whose row count is
// reported as the scenario result.
type plan struct {
	stmts []stmt
	main  int
	tx    bool
	// want, when > 0, is the exact row count expected from statement main.
	want int64
}

func q(sql string, args ...any) stmt { return stmt{sql: sql, args: args, query: true} }
func x(sql string, args ...any) stmt { return stmt{sql: sql, args: args} }

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

const releaseTickets = `UPDATE bilety SET status = 'wolny', rezerwacja_id = NULL WHERE rezerwacja_id = $1`

func (e *Engine) plan(p scenario.Params) (plan, error) {
	switch p.ID {
	case "C1", "C2":
		r := p.Reservation
		return plan{tx: true, main: 1, want: int64(len(p.TicketIDs)), stmts: []stmt{
			x(`INSERT INTO rezerwacje (id, klient_id, utworzono, status, wygasa_o, kwota, kod_rabatowy_id, metadane)
			   VALUES ($1, $2, $3, $4, $5, $6, NULL, $7)`, r.ID, r.CustomerID, r.CreatedAt, r.Status, r.ExpiresAt, r.Amount, r.Meta),
			x(`UPDATE bilety SET status = 'zarezerwowany', rezerwacja_id = $1 WHERE id = ANY($2) AND status = 'wolny'`, r.ID, p.TicketIDs),
		}}, nil
	case "C4":
		c := p.Customer
		return plan{stmts: []stmt{x(`INSERT INTO klienci (id, imie, nazwisko, email, telefon, miasto, utworzono, preferencje)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, customerRow(c)...)}}, nil
	case "C5":
		ev := p.Event
		var vals []string
		args := []any{}
		for i, pr := range ev.Prices {
			vals = append(vals, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)", i*5+1, i*5+2, i*5+3, i*5+4, i*5+5))
			args = append(args, priceRow(&pr)...)
		}
		return plan{tx: true, stmts: []stmt{
			x(`INSERT INTO wydarzenia (id, organizator_id, kategoria_id, obiekt_id, nazwa, data_rozpoczecia, status, metadane)
			   VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, eventRow(ev)...),
			x(`INSERT INTO cenniki (id, wydarzenie_id, sektor_id, cena, waluta) VALUES `+strings.Join(vals, ", "), args...),
		}}, nil
	case "C6":
		pm := p.Payment
		return plan{tx: true, stmts: []stmt{
			x(`INSERT INTO platnosci (id, rezerwacja_id, kwota, metoda, status, zaplacono, metadane) VALUES ($1, $2, $3, $4, $5, $6, $7)`, paymentRow(pm)...),
			x(`UPDATE rezerwacje SET status = 'oplacona', wygasa_o = NULL WHERE id = $1`, p.ReservationID),
		}}, nil
	case "R1":
		return plan{stmts: []stmt{q(`
			SELECT r.id, r.status, r.kwota, r.utworzono, b.id, b.cena, b.status, s.nazwa, m.rzad, m.numer
			FROM rezerwacje r
			LEFT JOIN bilety b ON b.rezerwacja_id = r.id
			LEFT JOIN miejsca m ON m.id = b.miejsce_id
			LEFT JOIN sektory s ON s.id = m.sektor_id
			WHERE r.id = $1`, p.ReservationID)}}, nil
	case "R2":
		return plan{stmts: []stmt{q(`
			SELECT b.id, s.nazwa, m.rzad, m.numer, b.cena
			FROM bilety b
			JOIN miejsca m ON m.id = b.miejsce_id
			JOIN sektory s ON s.id = m.sektor_id
			WHERE b.wydarzenie_id = $1 AND b.status = 'wolny'
			ORDER BY s.nazwa, m.rzad, m.numer`, p.EventID)}}, nil
	case "R3":
		return plan{stmts: []stmt{q(`
			SELECT w.id, w.nazwa, w.data_rozpoczecia, o.nazwa, k.nazwa
			FROM wydarzenia w
			JOIN obiekty o ON o.id = w.obiekt_id
			JOIN kategorie k ON k.id = w.kategoria_id
			WHERE o.miasto = $1 AND w.data_rozpoczecia >= $2 AND w.data_rozpoczecia < $3
			ORDER BY w.data_rozpoczecia`, p.City, p.From, p.To)}}, nil
	case "R4":
		return plan{stmts: []stmt{q(`
			SELECT r.id, r.utworzono, r.status, r.kwota, count(b.id) AS bilety
			FROM rezerwacje r
			LEFT JOIN bilety b ON b.rezerwacja_id = r.id
			WHERE r.klient_id = $1
			GROUP BY r.id
			ORDER BY r.utworzono DESC
			LIMIT 50`, p.CustomerID)}}, nil
	case "R5":
		return plan{stmts: []stmt{q(`
			SELECT w.id, w.nazwa, count(b.id) AS sprzedane, coalesce(sum(b.cena), 0) AS przychod
			FROM wydarzenia w
			LEFT JOIN bilety b ON b.wydarzenie_id = w.id AND b.status = 'sprzedany'
			WHERE w.organizator_id = $1
			GROUP BY w.id, w.nazwa`, p.OrganizerID)}}, nil
	case "R6":
		return plan{stmts: []stmt{q(`
			SELECT w.id, w.nazwa, t.sprzedane
			FROM (SELECT wydarzenie_id, count(*) AS sprzedane FROM bilety WHERE status = 'sprzedany'
			      GROUP BY wydarzenie_id ORDER BY sprzedane DESC LIMIT 10) t
			JOIN wydarzenia w ON w.id = t.wydarzenie_id
			ORDER BY t.sprzedane DESC`)}}, nil
	case "U1":
		return plan{stmts: []stmt{x(`UPDATE rezerwacje SET status = 'oplacona', wygasa_o = NULL WHERE id = $1`, p.ReservationID)}}, nil
	case "U2":
		return plan{want: 1, stmts: []stmt{x(`UPDATE bilety SET status = 'zablokowany', zablokowany_do = $2 WHERE id = $1 AND status = 'wolny'`, p.TicketIDs[0], p.NewDate)}}, nil
	case "U3":
		return plan{tx: true, stmts: []stmt{
			x(`UPDATE bilety b SET cena = round(b.cena * $3::numeric, 2)
			   FROM miejsca m
			   WHERE m.id = b.miejsce_id AND m.sektor_id = $2 AND b.wydarzenie_id = $1 AND b.status = 'wolny'`, p.EventID, p.SectorID, p.Factor),
			x(`UPDATE cenniki SET cena = round(cena * $3::numeric, 2) WHERE wydarzenie_id = $1 AND sektor_id = $2`, p.EventID, p.SectorID, p.Factor),
		}}, nil
	case "U4":
		c := p.Customer
		return plan{stmts: []stmt{x(`UPDATE klienci SET email = $2, telefon = $3, preferencje = preferencje || $4::jsonb WHERE id = $1`,
			c.ID, c.Email, c.Phone, mustJSON(map[string]any{"jezyk": c.Prefs.Language, "newsletter": c.Prefs.Newsletter}))}}, nil
	case "U5":
		return plan{stmts: []stmt{x(`UPDATE wydarzenia SET data_rozpoczecia = $2 WHERE id = $1`, p.EventID, p.NewDate)}}, nil
	case "U6":
		return plan{want: scenario.BulkReservations, stmts: []stmt{x(`
			WITH wygasle AS (
				UPDATE rezerwacje SET status = 'wygasla'
				WHERE status = 'oczekujaca' AND wygasa_o < $1
				RETURNING id
			)
			UPDATE bilety SET status = 'wolny', rezerwacja_id = NULL
			WHERE rezerwacja_id IN (SELECT id FROM wygasle)`, p.AsOf)}}, nil
	case "D1":
		return plan{tx: true, main: 2, stmts: []stmt{
			x(releaseTickets, p.ReservationID),
			x(`DELETE FROM platnosci WHERE rezerwacja_id = $1`, p.ReservationID),
			x(`DELETE FROM rezerwacje WHERE id = $1`, p.ReservationID),
		}}, nil
	case "D2":
		return plan{tx: true, main: 2, stmts: []stmt{
			x(`DELETE FROM bilety WHERE rezerwacja_id = $1`, p.ReservationID),
			x(`DELETE FROM platnosci WHERE rezerwacja_id = $1`, p.ReservationID),
			x(`DELETE FROM rezerwacje WHERE id = $1`, p.ReservationID),
		}}, nil
	case "D3":
		return plan{want: scenario.BulkReservations, stmts: []stmt{x(`DELETE FROM rezerwacje WHERE status = 'wygasla' AND wygasa_o < $1`, p.Cutoff)}}, nil
	case "D4":
		sub := `SELECT id FROM rezerwacje WHERE klient_id = $1`
		return plan{tx: true, main: 2, stmts: []stmt{
			x(`UPDATE bilety SET status = 'wolny', rezerwacja_id = NULL WHERE rezerwacja_id IN (`+sub+`)`, p.CustomerID),
			x(`DELETE FROM platnosci WHERE rezerwacja_id IN (`+sub+`)`, p.CustomerID),
			x(`DELETE FROM rezerwacje WHERE klient_id = $1`, p.CustomerID),
			x(`DELETE FROM klienci WHERE id = $1`, p.CustomerID),
		}}, nil
	case "D5":
		return plan{tx: true, stmts: []stmt{
			x(`DELETE FROM bilety WHERE wydarzenie_id = $1`, p.EventID),
			x(`DELETE FROM cenniki WHERE wydarzenie_id = $1`, p.EventID),
			x(`DELETE FROM wydarzenia WHERE id = $1 AND status = 'odwolane'`, p.EventID),
		}}, nil
	case "D6":
		old := `SELECT id FROM wydarzenia WHERE data_rozpoczecia < $1`
		return plan{tx: true, main: 2, stmts: []stmt{
			x(`DELETE FROM platnosci WHERE rezerwacja_id IN (SELECT id FROM rezerwacje WHERE utworzono < $1)`, p.Cutoff),
			x(`DELETE FROM rezerwacje WHERE utworzono < $1`, p.Cutoff),
			x(`DELETE FROM bilety WHERE wydarzenie_id IN (`+old+`)`, p.Cutoff),
			x(`DELETE FROM cenniki WHERE wydarzenie_id IN (`+old+`)`, p.Cutoff),
			x(`DELETE FROM wydarzenia WHERE data_rozpoczecia < $1`, p.Cutoff),
		}}, nil
	case "J1":
		return plan{stmts: []stmt{q(`SELECT id, nazwa, data_rozpoczecia FROM wydarzenia WHERE metadane @> $1::jsonb`,
			mustJSON(map[string]any{"gatunek": p.Genre, "tagi": []string{p.Tag}}))}}, nil
	case "J2":
		return plan{stmts: []stmt{q(`
			SELECT metadane ->> 'kanal' AS kanal, count(*), sum(kwota)
			FROM rezerwacje
			WHERE utworzono >= $1 AND utworzono < $2
			GROUP BY 1`, p.From, p.To)}}, nil
	case "J3":
		return plan{stmts: []stmt{x(`UPDATE wydarzenia SET metadane = jsonb_set(metadane, '{tagi}', (metadane -> 'tagi') || to_jsonb($2::text)) WHERE id = $1`,
			p.EventID, p.Tag)}}, nil
	}
	return plan{}, fmt.Errorf("postgres: scenario %s has no plan", p.ID)
}

func runStmt(ctx context.Context, db interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}, s stmt) (int64, error) {
	rows, err := db.Query(ctx, s.sql, s.args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var n int64
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if !s.query {
		n = rows.CommandTag().RowsAffected()
	}
	return n, nil
}

func (e *Engine) Run(ctx context.Context, p scenario.Params) (int64, error) {
	if p.ID == "C3" {
		return copyRows(ctx, e.pool, "bilety", ticketCols, p.Tickets, ticketRow)
	}
	pl, err := e.plan(p)
	if err != nil {
		return 0, err
	}
	var main int64
	exec := func(db interface {
		Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	}) error {
		for i, s := range pl.stmts {
			n, err := runStmt(ctx, db, s)
			if err != nil {
				return fmt.Errorf("%s statement %d: %w", p.ID, i, err)
			}
			if i == pl.main {
				main = n
			}
		}
		if pl.want > 0 && main != pl.want {
			return fmt.Errorf("%s: statement %d touched %d rows, want %d", p.ID, pl.main, main, pl.want)
		}
		return nil
	}
	if pl.tx {
		err = e.tx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error { return exec(tx) })
	} else {
		err = exec(e.pool)
	}
	return main, err
}

func (e *Engine) Explain(ctx context.Context, p scenario.Params) (storekit.Plan, error) {
	if p.ID == "C3" {
		return storekit.Plan{Format: "none", Statement: "COPY bilety FROM STDIN (binary)", Plan: "COPY has no query plan"}, nil
	}
	pl, err := e.plan(p)
	if err != nil {
		return storekit.Plan{}, err
	}
	main := pl.stmts[pl.main]
	out := storekit.Plan{Format: "text", Statement: strings.TrimSpace(main.sql)}
	// EXPLAIN ANALYZE executes the statement, so run it in a transaction
	// that is always rolled back.
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	for i := 0; i < pl.main; i++ {
		if _, err := runStmt(ctx, tx, pl.stmts[i]); err != nil {
			return out, err
		}
	}
	rows, err := tx.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS, VERBOSE OFF) "+main.sql, main.args...)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return out, err
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	out.Plan = b.String()
	return out, rows.Err()
}

func (e *Engine) Prepare(ctx context.Context, p scenario.Params) error {
	f := p.Fixture
	return e.tx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if _, err := copyRows(ctx, tx, "wydarzenia", eventCols, f.Events, eventRow); err != nil {
			return err
		}
		var prices []model.Price
		for _, ev := range f.Events {
			prices = append(prices, ev.Prices...)
		}
		if _, err := copyRows(ctx, tx, "cenniki", priceCols, prices, priceRow); err != nil {
			return err
		}
		if _, err := copyRows(ctx, tx, "bilety", ticketCols, f.Tickets, ticketRow); err != nil {
			return err
		}
		if _, err := copyRows(ctx, tx, "rezerwacje", reservationCols, f.Reservations, reservationRow); err != nil {
			return err
		}
		var pays []model.Payment
		for _, r := range f.Reservations {
			if r.Payment != nil {
				pays = append(pays, *r.Payment)
			}
		}
		_, err := copyRows(ctx, tx, "platnosci", paymentCols, pays, paymentRow)
		return err
	})
}
