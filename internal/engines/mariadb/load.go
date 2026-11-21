package mariadb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/ryczek02/ticketbench-contracts/pkg/datagen"
	"github.com/ryczek02/ticketbench-contracts/pkg/model"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
)

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// maxRowsPerInsert keeps one multi-row INSERT below a few MB.
const maxRowsPerInsert = 1000

func insertRows[T any](ctx context.Context, db execer, table string, cols []string, rows []T, fn func(*T) []any) (int64, error) {
	var total int64
	tuple := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(cols)), ", ") + ")"
	head := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(cols, ", "))
	err := storekit.Chunks(len(rows), maxRowsPerInsert, func(from, to int) error {
		var b strings.Builder
		b.WriteString(head)
		args := make([]any, 0, (to-from)*len(cols))
		for i := from; i < to; i++ {
			if i > from {
				b.WriteString(", ")
			}
			b.WriteString(tuple)
			args = append(args, fn(&rows[i])...)
		}
		res, err := db.ExecContext(ctx, b.String(), args...)
		if err != nil {
			return fmt.Errorf("insert %s: %w", table, err)
		}
		n, _ := res.RowsAffected()
		total += n
		return nil
	})
	return total, err
}

var (
	ticketCols      = []string{"id", "wydarzenie_id", "miejsce_id", "cena", "status", "rezerwacja_id", "zablokowany_do", "metadane"}
	reservationCols = []string{"id", "klient_id", "utworzono", "status", "wygasa_o", "kwota", "kod_rabatowy_id", "metadane"}
	paymentCols     = []string{"id", "rezerwacja_id", "kwota", "metoda", "status", "zaplacono", "metadane"}
	eventCols       = []string{"id", "organizator_id", "kategoria_id", "obiekt_id", "nazwa", "data_rozpoczecia", "status", "metadane"}
	priceCols       = []string{"id", "wydarzenie_id", "sektor_id", "cena", "waluta"}
	customerCols    = []string{"id", "imie", "nazwisko", "email", "telefon", "miasto", "utworzono", "preferencje"}
)

func nullID(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC()
}

func ticketRow(t *model.Ticket) []any {
	return []any{t.ID, t.EventID, t.SeatID, t.Price, t.Status, nullID(t.ReservationID), nullTime(t.HeldUntil), js(t.Meta)}
}

func reservationRow(r *model.Reservation) []any {
	return []any{r.ID, r.CustomerID, r.CreatedAt.UTC(), r.Status, nullTime(r.ExpiresAt), r.Amount, nullID(r.DiscountID), js(r.Meta)}
}

func paymentRow(p *model.Payment) []any {
	return []any{p.ID, p.ReservationID, p.Amount, p.Method, p.Status, p.PaidAt.UTC(), js(p.Meta)}
}

func eventRow(e *model.Event) []any {
	return []any{e.ID, e.OrganizerID, e.CategoryID, e.VenueID, e.Name, e.StartsAt.UTC(), e.Status, js(e.Meta)}
}

func priceRow(p *model.Price) []any {
	return []any{p.ID, p.EventID, p.SectorID, p.Price, p.Currency}
}

func customerRow(c *model.Customer) []any {
	return []any{c.ID, c.FirstName, c.LastName, c.Email, c.Phone, c.City, c.CreatedAt.UTC(), js(c.Prefs)}
}

// session returns a connection with unique and FK checks off for bulk loads.
func (e *Engine) session(ctx context.Context) (*sql.Conn, error) {
	c, err := e.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := c.ExecContext(ctx, "SET SESSION unique_checks = 0, foreign_key_checks = 0"); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

func (e *Engine) Load(ctx context.Context, d *datagen.Dataset, opts storekit.LoadOptions) (map[string]int64, error) {
	rows := map[string]int64{}
	db := e.db
	small := []struct {
		table string
		load  func() (int64, error)
	}{
		{"kategorie", func() (int64, error) {
			return insertRows(ctx, db, "kategorie", []string{"id", "nazwa"}, d.Categories, func(c *model.Category) []any { return []any{c.ID, c.Name} })
		}},
		{"organizatorzy", func() (int64, error) {
			return insertRows(ctx, db, "organizatorzy", []string{"id", "nazwa", "nip", "email"}, d.Organizers, func(o *model.Organizer) []any {
				return []any{o.ID, o.Name, o.TaxID, o.Email}
			})
		}},
		{"obiekty", func() (int64, error) {
			return insertRows(ctx, db, "obiekty", []string{"id", "nazwa", "miasto", "adres", "pojemnosc", "metadane"}, d.Venues, func(v *model.Venue) []any {
				return []any{v.ID, v.Name, v.City, v.Address, v.Capacity, js(v.Meta)}
			})
		}},
		{"sektory", func() (int64, error) {
			return insertRows(ctx, db, "sektory", []string{"id", "obiekt_id", "nazwa", "typ", "rzedy", "miejsc_w_rzedzie"}, storekit.AllSectors(d), func(s *model.Sector) []any {
				return []any{s.ID, s.VenueID, s.Name, s.Kind, s.Rows, s.PerRow}
			})
		}},
		{"miejsca", func() (int64, error) {
			return insertRows(ctx, db, "miejsca", []string{"id", "sektor_id", "rzad", "numer"}, storekit.AllSeats(d), func(s *model.Seat) []any {
				return []any{s.ID, s.SectorID, s.Row, s.Number}
			})
		}},
		{"wydarzenia", func() (int64, error) { return insertRows(ctx, db, "wydarzenia", eventCols, d.Events, eventRow) }},
		{"cenniki", func() (int64, error) {
			return insertRows(ctx, db, "cenniki", priceCols, storekit.AllPrices(d), priceRow)
		}},
		{"kody_rabatowe", func() (int64, error) {
			return insertRows(ctx, db, "kody_rabatowe", []string{"id", "kod", "procent", "wazny_od", "wazny_do", "limit_uzyc"}, d.Discounts, func(c *model.DiscountCode) []any {
				return []any{c.ID, c.Code, c.Percent, c.ValidFrom.UTC(), c.ValidUntil.UTC(), c.UsageLimit}
			})
		}},
	}
	for _, t := range small {
		n, err := t.load()
		if err != nil {
			return nil, err
		}
		rows[t.table] = n
	}
	if err := storekit.ParallelCustomers(ctx, d, opts.Workers, 20_000, func(ctx context.Context, cs []model.Customer) error {
		c, err := e.session(ctx)
		if err != nil {
			return err
		}
		defer c.Close()
		_, err = insertRows(ctx, c, "klienci", customerCols, cs, customerRow)
		return err
	}); err != nil {
		return nil, err
	}
	rows["klienci"] = d.Customers
	n, err := storekit.ParallelSales(ctx, d, opts.Workers, opts.BatchSize*4, func(ctx context.Context, b *storekit.SalesBatch) error {
		c, err := e.session(ctx)
		if err != nil {
			return err
		}
		defer c.Close()
		if _, err := insertRows(ctx, c, "bilety", ticketCols, b.Tickets, ticketRow); err != nil {
			return err
		}
		if _, err := insertRows(ctx, c, "rezerwacje", reservationCols, b.Reservations, reservationRow); err != nil {
			return err
		}
		_, err = insertRows(ctx, c, "platnosci", paymentCols, b.Payments, paymentRow)
		return err
	})
	if err != nil {
		return nil, err
	}
	rows["bilety"], rows["rezerwacje"], rows["platnosci"] = n.Tickets, n.Reservations, n.Payments
	if _, err := e.db.ExecContext(ctx, "ANALYZE TABLE "+strings.Join(tables, ", ")); err != nil {
		return nil, err
	}
	return rows, nil
}
