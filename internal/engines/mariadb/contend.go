package mariadb

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/ryczek02/ticketbench-contracts/pkg/datagen"
	"github.com/ryczek02/ticketbench-contracts/pkg/model"
	"github.com/ryczek02/ticketbench-contracts/pkg/scenario"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
)

func (e *Engine) ResetHot(ctx context.Context, _ *datagen.Dataset, tickets []int64) error {
	ph, args := in(tickets)
	return e.tx(ctx, nil, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM rezerwacje WHERE id > ?`, scenario.ContentionReservationBase); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE bilety SET status = 'wolny', rezerwacja_id = NULL, zablokowany_do = NULL WHERE id IN `+ph, args...)
		return err
	})
}

func (e *Engine) VerifyHot(ctx context.Context, tickets []int64) (sold, reservations int64, err error) {
	ph, args := in(tickets)
	args = append(args, scenario.ContentionReservationBase, scenario.ContentionReservationBase)
	err = e.db.QueryRowContext(ctx, `
		SELECT (SELECT count(*) FROM bilety WHERE id IN `+ph+` AND status = 'zarezerwowany' AND rezerwacja_id > ?),
		       (SELECT count(*) FROM rezerwacje WHERE id > ?)`, args...).Scan(&sold, &reservations)
	return
}

// conflict maps InnoDB concurrency errors to storekit.ErrConflict:
// 1213 deadlock, 1205 lock wait timeout (also returned by NOWAIT),
// 1020 record changed since last read (innodb_snapshot_isolation).
func conflict(err error) error {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		switch me.Number {
		case 1213, 1205, 1020, 3572:
			return storekit.ErrConflict
		}
	}
	return err
}

const insertReservation = `INSERT INTO rezerwacje (id, klient_id, utworzono, status, wygasa_o, kwota, kod_rabatowy_id, metadane)
	VALUES (?, ?, ?, 'oczekujaca', ?, ?, NULL, '{"kanal":"web"}')`

// TryBook implements the same strategies as the PostgreSQL adapter. InnoDB
// runs at REPEATABLE READ by default; SERIALIZABLE turns plain SELECTs into
// shared-lock reads, so conflicts show up as deadlocks.
func (e *Engine) TryBook(ctx context.Context, strategy string, b storekit.Booking) (bool, error) {
	exp := b.At.Add(15 * time.Minute).UTC()
	var opts *sql.TxOptions
	if strategy == "serializable" {
		opts = &sql.TxOptions{Isolation: sql.LevelSerializable}
	}
	ok := false
	err := e.tx(ctx, opts, func(tx *sql.Tx) error {
		switch strategy {
		case "cond":
			res, err := tx.ExecContext(ctx, `UPDATE bilety SET status = 'zarezerwowany', rezerwacja_id = ? WHERE id = ? AND status = 'wolny'`, b.ReservationID, b.TicketID)
			if err != nil {
				return err
			}
			if n, _ := res.RowsAffected(); n == 0 {
				return nil
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
			if err := tx.QueryRowContext(ctx, `SELECT status FROM bilety WHERE id = ?`+lock, b.TicketID).Scan(&st); err != nil {
				return err
			}
			if st != model.TicketFree {
				return nil
			}
			if _, err := tx.ExecContext(ctx, `UPDATE bilety SET status = 'zarezerwowany', rezerwacja_id = ? WHERE id = ?`, b.ReservationID, b.TicketID); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, insertReservation, b.ReservationID, b.CustomerID, b.At.UTC(), exp, b.Price); err != nil {
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
