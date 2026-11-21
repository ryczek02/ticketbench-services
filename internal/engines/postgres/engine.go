// Package postgres is the PostgreSQL adapter (pgx v5, otelpgx tracing).
package postgres

import (
	"context"
	_ "embed"
	"fmt"
	"regexp"
	"slices"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ryczek02/ticketbench-services/internal/storekit"
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
	pool *pgxpool.Pool
}

// Open connects with a pool of up to maxConns connections.
func Open(ctx context.Context, dsn string, maxConns int) (*Engine, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = int32(maxConns)
	cfg.ConnConfig.Tracer = otelpgx.NewTracer()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Engine{pool: pool}, nil
}

func (e *Engine) Name() string { return "postgres" }

func (e *Engine) Strategies() []string {
	return []string{"cond", "forupdate", "nowait", "serializable"}
}

func (e *Engine) Close() error { e.pool.Close(); return nil }

func (e *Engine) ServerVersion(ctx context.Context) (string, error) {
	var v string
	err := e.pool.QueryRow(ctx, "SHOW server_version").Scan(&v)
	return "PostgreSQL " + v, err
}

func (e *Engine) Reset(ctx context.Context) error {
	_, err := e.pool.Exec(ctx, schemaSQL)
	return err
}

var indexName = regexp.MustCompile(`(?i)CREATE INDEX (\w+)`)

func (e *Engine) SetIndexes(ctx context.Context, enabled bool) error {
	stmts := storekit.SQLStatements(indexesSQL)
	if !enabled {
		slices.Reverse(stmts)
		for _, s := range stmts {
			m := indexName.FindStringSubmatch(s)
			if _, err := e.pool.Exec(ctx, "DROP INDEX IF EXISTS "+m[1]); err != nil {
				return err
			}
		}
		_, err := e.pool.Exec(ctx, "ANALYZE")
		return err
	}
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SET maintenance_work_mem = '512MB'"); err != nil {
		return err
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	_, err = conn.Exec(ctx, "ANALYZE")
	return err
}

func (e *Engine) Stats(ctx context.Context) (storekit.Stats, error) {
	st := storekit.Stats{Rows: map[string]int64{}}
	err := e.pool.QueryRow(ctx, `
		SELECT coalesce(sum(pg_table_size(c.oid)), 0), coalesce(sum(pg_indexes_size(c.oid)), 0), pg_database_size(current_database())
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relkind = 'r'`).Scan(&st.DataBytes, &st.IndexBytes, &st.TotalBytes)
	if err != nil {
		return st, err
	}
	for _, t := range tables {
		var n int64
		if err := e.pool.QueryRow(ctx, "SELECT count(*) FROM "+t).Scan(&n); err != nil {
			return st, err
		}
		st.Rows[t] = n
	}
	return st, nil
}

// tx runs fn in a transaction with the given options.
func (e *Engine) tx(ctx context.Context, opts pgx.TxOptions, fn func(pgx.Tx) error) error {
	return pgx.BeginTxFunc(ctx, e.pool, opts, fn)
}
