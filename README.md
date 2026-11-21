# ticketbench-services

Server side of ticketbench: one gRPC store service per database and a
runner that executes benchmark plans. Part of
[ryczek02/ticketbench](https://github.com/ryczek02/ticketbench).

```
tbench CLI ──gRPC──> bench-runner ──gRPC──> store-postgres ──> PostgreSQL 17
                                    ├──────> store-mariadb  ──> MariaDB 11.8
                                    ├──────> store-mongo    ──> MongoDB 8 (replica set rs0)
                                    └──────> store-valkey   ──> Valkey 8
```

## Binaries

| Command | Listens on | Database driver |
|---|---|---|
| `bench-runner` | `:7000` | none, talks to the stores |
| `store-postgres` | `:7001` | pgx v5 + otelpgx |
| `store-mariadb` | `:7002` | go-sql-driver/mysql + otelsql |
| `store-mongo` | `:7003` | mongo-driver v2 + otelmongo |
| `store-valkey` | `:7004` | valkey-go + valkeyotel |

Each binary also accepts `version` and `healthcheck`. The images are
distroless, so Docker health checks call `/app healthcheck`, which asks the
gRPC health service of the running process.

## Configuration

| Variable | Default | Used by |
|---|---|---|
| `TB_LISTEN` | `:7000` runner, `:7001` stores | all |
| `TB_STORES` | `postgres=store-postgres:7001,...` | runner |
| `TB_POSTGRES_DSN` | `postgres://ticketbench:ticketbench@localhost:5432/ticketbench` | store-postgres |
| `TB_MARIADB_DSN` | `ticketbench:ticketbench@tcp(localhost:3306)/ticketbench` | store-mariadb |
| `TB_MONGO_URI` | `mongodb://localhost:27017/?replicaSet=rs0` | store-mongo |
| `TB_VALKEY_ADDR` | `localhost:6379` | store-valkey |
| `TB_MAX_CONNS` | `1100` | stores, pool size for the 1000-client test |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset (no export) | all |
| `TB_TRACE_RATIO` | `1` | all |

## How a measurement is taken

The runner calls `Execute(scenario, iteration)`. The store derives the
parameters from the dataset spec (same ids on every database), inserts the
fixture if the scenario needs one, then times only the database work with
`time.Now()` around `Engine.Run`. The runner records the end-to-end time of
the gRPC call next to it, so the transport overhead is visible but stays out
of the comparison.

Bulk loads generate the data inside the store from the seed. Nothing large
travels over gRPC, and the load response carries a checksum of the generated
rows.

## Data model per engine

PostgreSQL and MariaDB use the same 12 tables (`kategorie`, `organizatorzy`,
`obiekty`, `sektory`, `miejsca`, `wydarzenia`, `cenniki`, `klienci`,
`kody_rabatowe`, `rezerwacje`, `bilety`, `platnosci`) and the same 15
secondary indexes. Foreign keys are left out on purpose. InnoDB creates an
index for every foreign key, which would make the "no indexes" baseline
unequal between the two engines.

MongoDB stores events with their price list, venues with their sectors and
reservations with ticket snapshots and the payment. Tickets stay a separate
collection because they are the inventory that concurrent clients update.
Multi-document writes run in transactions.

Valkey keeps an event hash, a hash of tickets per event, a set of free
tickets per event, reservations as JSON strings and sales counters in a
sorted set. With indexes on, the store also maintains sets and sorted sets
under `ix:*`. With indexes off, lookups by attribute use `SCAN`. Lua scripts
make multi-key updates atomic.

## Contention test

`Contend` starts N goroutines inside the store, each with its own session,
racing for the same hot tickets of one sandbox event. A round ends when all
hot tickets are taken. The store then checks that the database holds exactly
one reservation per hot ticket.

| Engine | Strategies (first is the default) |
|---|---|
| PostgreSQL | `cond`, `forupdate`, `nowait`, `serializable` |
| MariaDB | `cond`, `forupdate`, `nowait`, `serializable` |
| MongoDB | `atomic`, `txn` |
| Valkey | `lua`, `setnx`, `watch` |

`cond` is `UPDATE ... WHERE status = 'wolny'`. `forupdate` and `nowait`
lock the row with `SELECT ... FOR UPDATE [NOWAIT]` first. `serializable`
retries serialization failures. `atomic` is a conditional `updateOne`. `lua`
does the check and the write in one script. `setnx` takes a TTL lock key
first, and `watch` uses optimistic `WATCH`/`MULTI`/`EXEC`.

## Tests

```
make test-unit   # no Docker needed
make test        # conformance suite on real databases via testcontainers-go
make bench       # Go micro-benchmarks
```

The conformance suite (`internal/enginetest`) loads 20 000 tickets and
checks the following on every engine:

- read results agree with an oracle computed from the generator;
- every scenario runs with indexes off and on;
- EXPLAIN works and leaves no fixture behind;
- the contention test is consistent for every strategy.

The runner test checks that a `traceparent` sent by the caller reaches the
stores.

## CI

GitHub Actions run vet, golangci-lint, unit tests, the conformance suite as a
matrix over the four engines and the Go benchmarks. They also build binaries
for linux/amd64, linux/arm64 and darwin/arm64, and multi-arch images pushed
to `ghcr.io/ryczek02/ticketbench-<service>`. A `v*` tag attaches the binaries
to a GitHub release.

## Authors

Łukasz Ryczko, Łukasz Pacholec. MIT license.
