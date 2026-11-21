package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ryczek02/ticketbench-contracts/pkg/datagen"
	"github.com/ryczek02/ticketbench-contracts/pkg/model"
	"github.com/ryczek02/ticketbench-contracts/pkg/scenario"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
)

func (e *Engine) ResetHot(ctx context.Context, _ *datagen.Dataset, tickets []int64) error {
	return e.tx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM rezerwacje WHERE id > $1`, scenario.ContentionReservationBase); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE bilety SET status = 'wolny', rezerwacja_id = NULL, zablokowany_do = NULL WHERE id = ANY($1)`, tickets)
		return err
	})
}

func (e *Engine) VerifyHot(ctx context.Context, tickets []int64) (sold, reservations int64, err error) {
	err = e.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM bilety WHERE id = ANY($1) AND status = 'zarezerwowany' AND rezerwacja_id > $2),
		       (SELECT count(*) FROM rezerwacje WHERE id > $2)`, tickets, scenario.ContentionReservationBase).Scan(&sold, &reservations)
	return
}

// conflict maps PostgreSQL concurrency errors to storekit.ErrConflict.
func conflict(err error) error {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		case "40001", "40P01", "55P03": // serialization_failure, deadlock_detected, lock_not_available
			return storekit.ErrConflict
		}
	}
	return err
}

const insertReservation = `INSERT INTO rezerwacje (id, klient_id, utworzono, status, wygasa_o, kwota, kod_rabatowy_id, metadane)
	VALUES ($1, $2, $3, 'oczekujaca', $4, $5, NULL, '{"kanal":"web"}')`

// TryBook implements the booking strategies:
//
//	cond          UPDATE ... WHERE status = 'wolny' (row lock, re-check after wait)
//	forupdate     SELECT ... FOR UPDATE, check status, UPDATE
//	nowait        SELECT ... FOR UPDATE NOWAIT, a locked row counts as a conflict
//	serializable  SERIALIZABLE transaction with plain SELECT, retried on 40001
func (e *Engine) TryBook(ctx context.Context, strategy string, b storekit.Booking) (bool, error) {
	exp := b.At.Add(15 * time.Minute)
	ok := false
	opts := pgx.TxOptions{}
	if strategy == "serializable" {
		opts.IsoLevel = pgx.Serializable
	}
	err := e.tx(ctx, opts, func(tx pgx.Tx) error {
		switch strategy {
		case "cond":
			tag, err := tx.Exec(ctx, `UPDATE bilety SET status = 'zarezerwowany', rezerwacja_id = $2 WHERE id = $1 AND status = 'wolny'`, b.TicketID, b.ReservationID)
			if err != nil || tag.RowsAffected() == 0 {
				return err
			}
		default:
			lock := ""
			switch strategy {
			case "forupdate":
				lock = " FOR UPDATE"
			case "nowait":
				lock = " FOR UPDATE NOWAIT"
			}
			var st string
			if err := tx.QueryRow(ctx, `SELECT status FROM bilety WHERE id = $1`+lock, b.TicketID).Scan(&st); err != nil {
				return err
			}
			if st != model.TicketFree {
				return nil
			}
			if _, err := tx.Exec(ctx, `UPDATE bilety SET status = 'zarezerwowany', rezerwacja_id = $2 WHERE id = $1`, b.TicketID, b.ReservationID); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, insertReservation, b.ReservationID, b.CustomerID, b.At, exp, b.Price); err != nil {
			return err
		}
		ok = true
		return nil
	})
	if err != nil {
		return false, conflict(err)
	}
	return ok, nil
}
