// Package mariadb is the MariaDB adapter (go-sql-driver/mysql, otelsql).
package mariadb

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/XSAM/otelsql"
	"github.com/go-sql-driver/mysql"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

//go:embed schema.sql
var schemaSQL string

//go:embed indexes.sql
var indexesSQL string

var tables = []string{
	"kategorie", "organizatorzy", "obiekty", "sektory", "miejsca", "wydarzenia",
	"cenniki", "klienci", "kody_rabatowe", "rezerwacje", "bilety", "platnosci",
}

// Engine implements storekit.Engine.
type Engine struct {
	db *sql.DB
}

// TLSConfigName is the name to use in the DSN (tls=tb) for connections
// verified against the CA in TB_TLS_CA.
const TLSConfigName = "tb"

// Open connects to MariaDB. The DSN must use parseTime=true and loc=UTC.
func Open(ctx context.Context, dsn string, maxConns int) (*Engine, error) {
	tlsCfg, err := storekit.TLSConfigFromEnv()
	if err != nil {
		return nil, err
	}
	if tlsCfg != nil {
		if err := mysql.RegisterTLSConfig(TLSConfigName, tlsCfg); err != nil {
			return nil, err
		}
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	// One round trip per statement, like pgx with its statement cache.
	cfg.InterpolateParams = true
	cfg.MaxAllowedPacket = 64 << 20
	db, err := otelsql.Open("mysql", cfg.FormatDSN(),
		otelsql.WithAttributes(semconv.DBSystemNameMariaDB),
		otelsql.WithSpanOptions(otelsql.SpanOptions{OmitConnResetSession: true, OmitRows: true, OmitConnPrepare: true}),
	)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxIdleTime(10 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &Engine{db: db}, nil
}

func (e *Engine) Name() string { return "mariadb" }

func (e *Engine) Strategies() []string {
	return []string{"cond", "forupdate", "nowait", "serializable"}
}

func (e *Engine) Close() error { return e.db.Close() }

func (e *Engine) ServerVersion(ctx context.Context) (string, error) {
	var v string
	err := e.db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&v)
	return "MariaDB " + v, err
}

func (e *Engine) Reset(ctx context.Context) error {
	for _, s := range storekit.SQLStatements(schemaSQL) {
		if _, err := e.db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("%.60s: %w", s, err)
		}
	}
	return nil
}

var indexDef = regexp.MustCompile(`(?i)CREATE INDEX (\w+) ON (\w+)`)

func (e *Engine) SetIndexes(ctx context.Context, enabled bool) error {
	stmts := storekit.SQLStatements(indexesSQL)
	if !enabled {
		slices.Reverse(stmts)
	}
	for _, s := range stmts {
		if !enabled {
			m := indexDef.FindStringSubmatch(s)
			s = fmt.Sprintf("DROP INDEX IF EXISTS %s ON %s", m[1], m[2])
		}
		if _, err := e.db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	_, err := e.db.ExecContext(ctx, "ANALYZE TABLE "+strings.Join(tables, ", "))
	return err
}

func (e *Engine) Stats(ctx context.Context) (storekit.Stats, error) {
	st := storekit.Stats{Rows: map[string]int64{}}
	err := e.db.QueryRowContext(ctx, `
		SELECT coalesce(sum(data_length), 0), coalesce(sum(index_length), 0)
		FROM information_schema.tables WHERE table_schema = DATABASE()`).Scan(&st.DataBytes, &st.IndexBytes)
	if err != nil {
		return st, err
	}
	st.TotalBytes = st.DataBytes + st.IndexBytes
	for _, t := range tables {
		var n int64
		if err := e.db.QueryRowContext(ctx, "SELECT count(*) FROM "+t).Scan(&n); err != nil {
			return st, err
		}
		st.Rows[t] = n
	}
	return st, nil
}

func (e *Engine) tx(ctx context.Context, opts *sql.TxOptions, fn func(*sql.Tx) error) error {
	tx, err := e.db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func js(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// in returns "(?, ?, ?)" and the arguments for an IN list.
func in(ids []int64) (string, []any) {
	ph := strings.TrimSuffix(strings.Repeat("?, ", len(ids)), ", ")
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return "(" + ph + ")", args
}
