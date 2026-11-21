package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/ryczek02/ticketbench-contracts/pkg/datagen"
	"github.com/ryczek02/ticketbench-contracts/pkg/model"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
)

type copier interface {
	CopyFrom(ctx context.Context, table pgx.Identifier, cols []string, src pgx.CopyFromSource) (int64, error)
}

func copyRows[T any](ctx context.Context, c copier, table string, cols []string, rows []T, fn func(*T) []any) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	return c.CopyFrom(ctx, pgx.Identifier{table}, cols, pgx.CopyFromSlice(len(rows), func(i int) ([]any, error) {
		return fn(&rows[i]), nil
	}))
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

func ticketRow(t *model.Ticket) []any {
	return []any{t.ID, t.EventID, t.SeatID, t.Price, t.Status, nullID(t.ReservationID), t.HeldUntil, t.Meta}
}

func reservationRow(r *model.Reservation) []any {
	return []any{r.ID, r.CustomerID, r.CreatedAt, r.Status, r.ExpiresAt, r.Amount, nullID(r.DiscountID), r.Meta}
}

func paymentRow(p *model.Payment) []any {
	return []any{p.ID, p.ReservationID, p.Amount, p.Method, p.Status, p.PaidAt, p.Meta}
}

func eventRow(e *model.Event) []any {
	return []any{e.ID, e.OrganizerID, e.CategoryID, e.VenueID, e.Name, e.StartsAt, e.Status, e.Meta}
}

func priceRow(p *model.Price) []any {
	return []any{p.ID, p.EventID, p.SectorID, p.Price, p.Currency}
}

func customerRow(c *model.Customer) []any {
	return []any{c.ID, c.FirstName, c.LastName, c.Email, c.Phone, c.City, c.CreatedAt, c.Prefs}
}

func (e *Engine) Load(ctx context.Context, d *datagen.Dataset, opts storekit.LoadOptions) (map[string]int64, error) {
	rows := map[string]int64{}
	p := e.pool
	small := []struct {
		table string
		load  func() (int64, error)
	}{
		{"kategorie", func() (int64, error) {
			return copyRows(ctx, p, "kategorie", []string{"id", "nazwa"}, d.Categories, func(c *model.Category) []any { return []any{c.ID, c.Name} })
		}},
		{"organizatorzy", func() (int64, error) {
			return copyRows(ctx, p, "organizatorzy", []string{"id", "nazwa", "nip", "email"}, d.Organizers, func(o *model.Organizer) []any {
				return []any{o.ID, o.Name, o.TaxID, o.Email}
			})
		}},
		{"obiekty", func() (int64, error) {
			return copyRows(ctx, p, "obiekty", []string{"id", "nazwa", "miasto", "adres", "pojemnosc", "metadane"}, d.Venues, func(v *model.Venue) []any {
				return []any{v.ID, v.Name, v.City, v.Address, v.Capacity, v.Meta}
			})
		}},
		{"sektory", func() (int64, error) {
			return copyRows(ctx, p, "sektory", []string{"id", "obiekt_id", "nazwa", "typ", "rzedy", "miejsc_w_rzedzie"}, storekit.AllSectors(d), func(s *model.Sector) []any {
				return []any{s.ID, s.VenueID, s.Name, s.Kind, s.Rows, s.PerRow}
			})
		}},
		{"miejsca", func() (int64, error) {
			return copyRows(ctx, p, "miejsca", []string{"id", "sektor_id", "rzad", "numer"}, storekit.AllSeats(d), func(s *model.Seat) []any {
				return []any{s.ID, s.SectorID, s.Row, s.Number}
			})
		}},
		{"wydarzenia", func() (int64, error) { return copyRows(ctx, p, "wydarzenia", eventCols, d.Events, eventRow) }},
		{"cenniki", func() (int64, error) { return copyRows(ctx, p, "cenniki", priceCols, storekit.AllPrices(d), priceRow) }},
		{"kody_rabatowe", func() (int64, error) {
			return copyRows(ctx, p, "kody_rabatowe", []string{"id", "kod", "procent", "wazny_od", "wazny_do", "limit_uzyc"}, d.Discounts, func(c *model.DiscountCode) []any {
				return []any{c.ID, c.Code, c.Percent, c.ValidFrom, c.ValidUntil, c.UsageLimit}
			})
		}},
	}
	for _, t := range small {
		n, err := t.load()
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", t.table, err)
		}
		rows[t.table] = n
	}
	if err := storekit.ParallelCustomers(ctx, d, opts.Workers, 50_000, func(ctx context.Context, cs []model.Customer) error {
		_, err := copyRows(ctx, p, "klienci", customerCols, cs, customerRow)
		return err
	}); err != nil {
		return nil, err
	}
	rows["klienci"] = d.Customers
	n, err := storekit.ParallelSales(ctx, d, opts.Workers, opts.BatchSize*10, func(ctx context.Context, b *storekit.SalesBatch) error {
		if _, err := copyRows(ctx, p, "bilety", ticketCols, b.Tickets, ticketRow); err != nil {
			return err
		}
		if _, err := copyRows(ctx, p, "rezerwacje", reservationCols, b.Reservations, reservationRow); err != nil {
			return err
		}
		_, err := copyRows(ctx, p, "platnosci", paymentCols, b.Payments, paymentRow)
		return err
	})
	if err != nil {
		return nil, err
	}
	rows["bilety"], rows["rezerwacje"], rows["platnosci"] = n.Tickets, n.Reservations, n.Payments
	if _, err := p.Exec(ctx, "VACUUM ANALYZE"); err != nil {
		return nil, err
	}
	return rows, nil
}
