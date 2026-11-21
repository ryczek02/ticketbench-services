package mariadb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/ryczek02/ticketbench-contracts/pkg/model"
	"github.com/ryczek02/ticketbench-contracts/pkg/scenario"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
)

type stmt struct {
	sql   string
	args  []any
	query bool
}

// plan mirrors the PostgreSQL adapter: main is the statement that is
// explained and whose row count is the scenario result.
type plan struct {
	stmts []stmt
	main  int
	tx    bool
	want  int64
}

func q(sql string, args ...any) stmt { return stmt{sql: sql, args: args, query: true} }
func x(sql string, args ...any) stmt { return stmt{sql: sql, args: args} }

func (e *Engine) plan(p scenario.Params) (plan, error) {
	switch p.ID {
	case "C1", "C2":
		r := p.Reservation
		ph, ids := in(p.TicketIDs)
		return plan{tx: true, main: 1, want: int64(len(p.TicketIDs)), stmts: []stmt{
			x(`INSERT INTO rezerwacje (id, klient_id, utworzono, status, wygasa_o, kwota, kod_rabatowy_id, metadane)
			   VALUES (?, ?, ?, ?, ?, ?, NULL, ?)`, r.ID, r.CustomerID, r.CreatedAt.UTC(), r.Status, nullTime(r.ExpiresAt), r.Amount, js(r.Meta)),
			x(`UPDATE bilety SET status = 'zarezerwowany', rezerwacja_id = ? WHERE id IN `+ph+` AND status = 'wolny'`, append([]any{r.ID}, ids...)...),
		}}, nil
	case "C4":
		return plan{stmts: []stmt{x(`INSERT INTO klienci (id, imie, nazwisko, email, telefon, miasto, utworzono, preferencje)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, customerRow(p.Customer)...)}}, nil
	case "C5":
		ev := p.Event
		var vals []string
		var args []any
		for _, pr := range ev.Prices {
			vals = append(vals, "(?, ?, ?, ?, ?)")
			args = append(args, priceRow(&pr)...)
		}
		return plan{tx: true, stmts: []stmt{
			x(`INSERT INTO wydarzenia (id, organizator_id, kategoria_id, obiekt_id, nazwa, data_rozpoczecia, status, metadane)
			   VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, eventRow(ev)...),
			x(`INSERT INTO cenniki (id, wydarzenie_id, sektor_id, cena, waluta) VALUES `+strings.Join(vals, ", "), args...),
		}}, nil
	case "C6":
		return plan{tx: true, stmts: []stmt{
			x(`INSERT INTO platnosci (id, rezerwacja_id, kwota, metoda, status, zaplacono, metadane) VALUES (?, ?, ?, ?, ?, ?, ?)`, paymentRow(p.Payment)...),
			x(`UPDATE rezerwacje SET status = 'oplacona', wygasa_o = NULL WHERE id = ?`, p.ReservationID),
		}}, nil
	case "R1":
		return plan{stmts: []stmt{q(`
			SELECT r.id, r.status, r.kwota, r.utworzono, b.id, b.cena, b.status, s.nazwa, m.rzad, m.numer
			FROM rezerwacje r
			LEFT JOIN bilety b ON b.rezerwacja_id = r.id
			LEFT JOIN miejsca m ON m.id = b.miejsce_id
			LEFT JOIN sektory s ON s.id = m.sektor_id
			WHERE r.id = ?`, p.ReservationID)}}, nil
	case "R2":
		return plan{stmts: []stmt{q(`
			SELECT b.id, s.nazwa, m.rzad, m.numer, b.cena
			FROM bilety b
			JOIN miejsca m ON m.id = b.miejsce_id
			JOIN sektory s ON s.id = m.sektor_id
			WHERE b.wydarzenie_id = ? AND b.status = 'wolny'
			ORDER BY s.nazwa, m.rzad, m.numer`, p.EventID)}}, nil
	case "R3":
		return plan{stmts: []stmt{q(`
			SELECT w.id, w.nazwa, w.data_rozpoczecia, o.nazwa, k.nazwa
			FROM wydarzenia w
			JOIN obiekty o ON o.id = w.obiekt_id
			JOIN kategorie k ON k.id = w.kategoria_id
			WHERE o.miasto = ? AND w.data_rozpoczecia >= ? AND w.data_rozpoczecia < ?
			ORDER BY w.data_rozpoczecia`, p.City, p.From.UTC(), p.To.UTC())}}, nil
	case "R4":
		return plan{stmts: []stmt{q(`
			SELECT r.id, r.utworzono, r.status, r.kwota, count(b.id) AS bilety
			FROM rezerwacje r
			LEFT JOIN bilety b ON b.rezerwacja_id = r.id
			WHERE r.klient_id = ?
			GROUP BY r.id, r.utworzono, r.status, r.kwota
			ORDER BY r.utworzono DESC
			LIMIT 50`, p.CustomerID)}}, nil
	case "R5":
		return plan{stmts: []stmt{q(`
			SELECT w.id, w.nazwa, count(b.id) AS sprzedane, coalesce(sum(b.cena), 0) AS przychod
			FROM wydarzenia w
			LEFT JOIN bilety b ON b.wydarzenie_id = w.id AND b.status = 'sprzedany'
			WHERE w.organizator_id = ?
			GROUP BY w.id, w.nazwa`, p.OrganizerID)}}, nil
	case "R6":
		return plan{stmts: []stmt{q(`
			SELECT w.id, w.nazwa, t.sprzedane
			FROM (SELECT wydarzenie_id, count(*) AS sprzedane FROM bilety WHERE status = 'sprzedany'
			      GROUP BY wydarzenie_id ORDER BY sprzedane DESC LIMIT 10) t
			JOIN wydarzenia w ON w.id = t.wydarzenie_id
			ORDER BY t.sprzedane DESC`)}}, nil
	case "U1":
		return plan{stmts: []stmt{x(`UPDATE rezerwacje SET status = 'oplacona', wygasa_o = NULL WHERE id = ?`, p.ReservationID)}}, nil
	case "U2":
		return plan{want: 1, stmts: []stmt{x(`UPDATE bilety SET status = 'zablokowany', zablokowany_do = ? WHERE id = ? AND status = 'wolny'`, p.NewDate.UTC(), p.TicketIDs[0])}}, nil
	case "U3":
		return plan{tx: true, stmts: []stmt{
			x(`UPDATE bilety b JOIN miejsca m ON m.id = b.miejsce_id
			   SET b.cena = round(b.cena * ?, 2)
			   WHERE m.sektor_id = ? AND b.wydarzenie_id = ? AND b.status = 'wolny'`, p.Factor, p.SectorID, p.EventID),
			x(`UPDATE cenniki SET cena = round(cena * ?, 2) WHERE wydarzenie_id = ? AND sektor_id = ?`, p.Factor, p.EventID, p.SectorID),
		}}, nil
	case "U4":
		c := p.Customer
		return plan{stmts: []stmt{x(`UPDATE klienci SET email = ?, telefon = ?, preferencje = JSON_MERGE_PATCH(preferencje, ?) WHERE id = ?`,
			c.Email, c.Phone, js(map[string]any{"jezyk": c.Prefs.Language, "newsletter": c.Prefs.Newsletter}), c.ID)}}, nil
	case "U5":
		return plan{stmts: []stmt{x(`UPDATE wydarzenia SET data_rozpoczecia = ? WHERE id = ?`, p.NewDate.UTC(), p.EventID)}}, nil
	case "U6":
		// Release the tickets first (join on the still pending reservations),
		// then mark the reservations as expired.
		return plan{tx: true, want: scenario.BulkReservations, stmts: []stmt{
			x(`UPDATE bilety b JOIN rezerwacje r ON r.id = b.rezerwacja_id
			   SET b.status = 'wolny', b.rezerwacja_id = NULL
			   WHERE r.status = 'oczekujaca' AND r.wygasa_o < ?`, p.AsOf.UTC()),
			x(`UPDATE rezerwacje SET status = 'wygasla' WHERE status = 'oczekujaca' AND wygasa_o < ?`, p.AsOf.UTC()),
		}}, nil
	case "D1":
		return plan{tx: true, main: 2, stmts: []stmt{
			x(`UPDATE bilety SET status = 'wolny', rezerwacja_id = NULL WHERE rezerwacja_id = ?`, p.ReservationID),
			x(`DELETE FROM platnosci WHERE rezerwacja_id = ?`, p.ReservationID),
			x(`DELETE FROM rezerwacje WHERE id = ?`, p.ReservationID),
		}}, nil
	case "D2":
		return plan{tx: true, main: 2, stmts: []stmt{
			x(`DELETE FROM bilety WHERE rezerwacja_id = ?`, p.ReservationID),
			x(`DELETE FROM platnosci WHERE rezerwacja_id = ?`, p.ReservationID),
			x(`DELETE FROM rezerwacje WHERE id = ?`, p.ReservationID),
		}}, nil
	case "D3":
		return plan{want: scenario.BulkReservations, stmts: []stmt{x(`DELETE FROM rezerwacje WHERE status = 'wygasla' AND wygasa_o < ?`, p.Cutoff.UTC())}}, nil
	case "D4":
		sub := `SELECT id FROM rezerwacje WHERE klient_id = ?`
		return plan{tx: true, main: 2, stmts: []stmt{
			x(`UPDATE bilety SET status = 'wolny', rezerwacja_id = NULL WHERE rezerwacja_id IN (`+sub+`)`, p.CustomerID),
			x(`DELETE FROM platnosci WHERE rezerwacja_id IN (`+sub+`)`, p.CustomerID),
			x(`DELETE FROM rezerwacje WHERE klient_id = ?`, p.CustomerID),
			x(`DELETE FROM klienci WHERE id = ?`, p.CustomerID),
		}}, nil
	case "D5":
		return plan{tx: true, stmts: []stmt{
			x(`DELETE FROM bilety WHERE wydarzenie_id = ?`, p.EventID),
			x(`DELETE FROM cenniki WHERE wydarzenie_id = ?`, p.EventID),
			x(`DELETE FROM wydarzenia WHERE id = ? AND status = 'odwolane'`, p.EventID),
		}}, nil
	case "D6":
		old := `SELECT id FROM wydarzenia WHERE data_rozpoczecia < ?`
		c := p.Cutoff.UTC()
		return plan{tx: true, main: 2, stmts: []stmt{
			x(`DELETE FROM platnosci WHERE rezerwacja_id IN (SELECT id FROM rezerwacje WHERE utworzono < ?)`, c),
			x(`DELETE FROM rezerwacje WHERE utworzono < ?`, c),
			x(`DELETE FROM bilety WHERE wydarzenie_id IN (`+old+`)`, c),
			x(`DELETE FROM cenniki WHERE wydarzenie_id IN (`+old+`)`, c),
			x(`DELETE FROM wydarzenia WHERE data_rozpoczecia < ?`, c),
		}}, nil
	case "J1":
		return plan{stmts: []stmt{q(`SELECT id, nazwa, data_rozpoczecia FROM wydarzenia
			WHERE gatunek = ? AND JSON_CONTAINS(metadane, ?, '$.tagi')`, p.Genre, js(p.Tag))}}, nil
	case "J2":
		return plan{stmts: []stmt{q(`
			SELECT JSON_UNQUOTE(JSON_EXTRACT(metadane, '$.kanal')) AS kanal, count(*), sum(kwota)
			FROM rezerwacje
			WHERE utworzono >= ? AND utworzono < ?
			GROUP BY kanal`, p.From.UTC(), p.To.UTC())}}, nil
	case "J3":
		return plan{stmts: []stmt{x(`UPDATE wydarzenia SET metadane = JSON_ARRAY_APPEND(metadane, '$.tagi', ?) WHERE id = ?`, p.Tag, p.EventID)}}, nil
	}
	return plan{}, fmt.Errorf("mariadb: scenario %s has no plan", p.ID)
}

type querier interface {
	execer
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func runStmt(ctx context.Context, db querier, s stmt) (int64, error) {
	if !s.query {
		res, err := db.ExecContext(ctx, s.sql, s.args...)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	}
	rows, err := db.QueryContext(ctx, s.sql, s.args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var n int64
	for rows.Next() {
		n++
	}
	return n, rows.Err()
}

func (e *Engine) Run(ctx context.Context, p scenario.Params) (int64, error) {
	if p.ID == "C3" {
		return insertRows(ctx, e.db, "bilety", ticketCols, p.Tickets, ticketRow)
	}
	pl, err := e.plan(p)
	if err != nil {
		return 0, err
	}
	var main int64
	exec := func(db querier) error {
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
		err = e.tx(ctx, nil, func(tx *sql.Tx) error { return exec(tx) })
	} else {
		err = exec(e.db)
	}
	return main, err
}

func (e *Engine) Explain(ctx context.Context, p scenario.Params) (storekit.Plan, error) {
	if p.ID == "C3" {
		return storekit.Plan{Format: "none", Statement: "INSERT INTO bilety ... VALUES (...), (...)", Plan: "multi-row INSERT has no query plan"}, nil
	}
	pl, err := e.plan(p)
	if err != nil {
		return storekit.Plan{}, err
	}
	main := pl.stmts[pl.main]
	out := storekit.Plan{Format: "json", Statement: strings.TrimSpace(main.sql)}
	// ANALYZE executes the statement, so keep it in a rolled back transaction.
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer func() { _ = tx.Rollback() }()
	for i := 0; i < pl.main; i++ {
		if _, err := runStmt(ctx, tx, pl.stmts[i]); err != nil {
			return out, err
		}
	}
	verb := "ANALYZE FORMAT=JSON "
	if strings.HasPrefix(strings.TrimSpace(main.sql), "INSERT") {
		verb = "EXPLAIN FORMAT=JSON "
	}
	if err := tx.QueryRowContext(ctx, verb+main.sql, main.args...).Scan(&out.Plan); err != nil {
		return out, err
	}
	return out, nil
}

func (e *Engine) Prepare(ctx context.Context, p scenario.Params) error {
	f := p.Fixture
	return e.tx(ctx, nil, func(tx *sql.Tx) error {
		if _, err := insertRows(ctx, tx, "wydarzenia", eventCols, f.Events, eventRow); err != nil {
			return err
		}
		var prices []model.Price
		for _, ev := range f.Events {
			prices = append(prices, ev.Prices...)
		}
		if _, err := insertRows(ctx, tx, "cenniki", priceCols, prices, priceRow); err != nil {
			return err
		}
		if _, err := insertRows(ctx, tx, "bilety", ticketCols, f.Tickets, ticketRow); err != nil {
			return err
		}
		if _, err := insertRows(ctx, tx, "rezerwacje", reservationCols, f.Reservations, reservationRow); err != nil {
			return err
		}
		var pays []model.Payment
		for _, r := range f.Reservations {
			if r.Payment != nil {
				pays = append(pays, *r.Payment)
			}
		}
		_, err := insertRows(ctx, tx, "platnosci", paymentCols, pays, paymentRow)
		return err
	})
}
