package storekit

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	tbv1 "github.com/ryczek02/ticketbench-contracts/gen/ticketbench/v1"
	"github.com/ryczek02/ticketbench-contracts/pkg/datagen"
	"github.com/ryczek02/ticketbench-contracts/pkg/scenario"
	"github.com/ryczek02/ticketbench-contracts/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxRetries bounds how often a client retries the same ticket after a
// retryable conflict (serialization failure, deadlock, WATCH abort).
const maxRetries = 5

// Contend runs the seat contention test: `clients` goroutines, each with its
// own database session, race for the same `hot_tickets` seats of one sandbox
// event. A round ends when every hot ticket is taken.
func (s *Server) Contend(ctx context.Context, req *tbv1.ContendRequest) (*tbv1.ContendResponse, error) {
	d, err := s.dataset(req.GetDataset())
	if err != nil {
		return nil, err
	}
	strategy, err := s.strategy(req.GetStrategy())
	if err != nil {
		return nil, err
	}
	eng, err := s.engine(ctx, req.GetProfile())
	if err != nil {
		return nil, err
	}
	clients := int(req.GetClients())
	hotN := int(req.GetHotTickets())
	rounds := int(req.GetRounds())
	if clients < 1 || hotN < 1 || rounds < 1 {
		return nil, status.Error(codes.InvalidArgument, "clients, hot_tickets and rounds must be positive")
	}
	hot, err := scenario.HotTickets(d, hotN)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	prices := map[int64]float64{}
	for _, t := range scenario.SandboxTickets(d, 0)[:hotN] {
		prices[t.ID] = t.Price
	}

	s.busy.Lock()
	defer s.busy.Unlock()
	ctx, span := s.tracer.Start(ctx, "store.contend", trace.WithAttributes(s.attrs(
		attribute.Int("tb.clients", clients),
		attribute.Int("tb.hot_tickets", hotN),
		attribute.String("tb.strategy", strategy),
		attribute.String("tb.profile", req.GetProfile()),
	)...))
	defer span.End()

	resp := &tbv1.ContendResponse{}
	for round := 1; round <= rounds; round++ {
		r, err := s.contendRound(ctx, eng, d, strategy, clients, hot, prices, round, req.GetTraceRatio())
		if err != nil {
			return nil, fail(span, err)
		}
		slog.InfoContext(ctx, "contention round", "clients", clients, "strategy", strategy, "round", round,
			"took", time.Duration(r.DurationNs).String(), "attempts", r.Attempts, "conflicts", r.Conflicts,
			"errors", r.Errors, "consistent", r.Consistent)
		resp.Rounds = append(resp.Rounds, r)
	}
	return resp, nil
}

func (s *Server) contendRound(ctx context.Context, eng Engine, d *datagen.Dataset, strategy string, clients int, hot []int64,
	prices map[int64]float64, round int, traceRatio float64) (*tbv1.ContendRound, error) {

	rctx, rspan := s.tracer.Start(ctx, "contend.round", trace.WithAttributes(attribute.Int("tb.round", round)))
	defer rspan.End()
	if err := eng.ResetHot(telemetry.Quiet(rctx), d, hot); err != nil {
		return nil, err
	}

	var (
		successes, conflicts, errs, attempts atomic.Int64
		nextID                               atomic.Int64
		mu                                   sync.Mutex
		latencies                            = make([]int64, 0, len(hot)*4)
		firstErr                             error
	)
	nextID.Store(scenario.ContentionReservationBase)
	target := int64(len(hot))
	evID := scenario.HotEventID(d)
	attrs := metric.WithAttributes(s.attrs(attribute.String("tb.strategy", strategy), attribute.Int("tb.clients", clients))...)

	var wg sync.WaitGroup
	start := time.Now()
	for c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(d.Spec.Seed, uint64(round)<<32|uint64(c)))
			local := make([]int64, 0, 16)
			customer := 1 + int64(c)%d.Customers
			for _, i := range rng.Perm(len(hot)) {
				if successes.Load() >= target || rctx.Err() != nil {
					break
				}
				ticket := hot[i]
				for try := 0; try <= maxRetries; try++ {
					actx := telemetry.Quiet(rctx)
					var aspan trace.Span
					if traceRatio > 0 && rng.Float64() < traceRatio {
						actx, aspan = s.tracer.Start(rctx, "contend.attempt", trace.WithAttributes(attribute.Int64("tb.ticket", ticket)))
					}
					t0 := time.Now()
					ok, err := eng.TryBook(actx, strategy, Booking{
						TicketID: ticket, EventID: evID, ReservationID: nextID.Add(1),
						CustomerID: customer, Price: prices[ticket], At: datagen.T0,
					})
					lat := time.Since(t0)
					if aspan != nil {
						aspan.SetAttributes(attribute.Bool("tb.ok", ok))
						aspan.End()
					}
					attempts.Add(1)
					local = append(local, lat.Nanoseconds())
					s.attemptHist.Record(rctx, float64(lat.Microseconds())/1000, attrs)
					switch {
					case err == nil && ok:
						successes.Add(1)
					case err == nil:
						conflicts.Add(1)
						s.conflicts.Add(rctx, 1, attrs)
					case errors.Is(err, ErrConflict):
						conflicts.Add(1)
						s.conflicts.Add(rctx, 1, attrs)
						continue // retry the same ticket
					default:
						errs.Add(1)
						mu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						mu.Unlock()
					}
					break
				}
			}
			mu.Lock()
			latencies = append(latencies, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	took := time.Since(start)
	if firstErr != nil {
		slog.WarnContext(rctx, "contention errors", "count", errs.Load(), "first", firstErr.Error())
	}

	sold, reservations, err := eng.VerifyHot(telemetry.Quiet(rctx), hot)
	if err != nil {
		return nil, err
	}
	out := &tbv1.ContendRound{
		Round:      int32(round),
		DurationNs: took.Nanoseconds(),
		Attempts:   attempts.Load(),
		Successes:  successes.Load(),
		Conflicts:  conflicts.Load(),
		Errors:     errs.Load(),
		Consistent: sold == target && reservations == target && successes.Load() == target,
	}
	fillPercentiles(out, latencies)
	rspan.SetAttributes(attribute.Bool("tb.consistent", out.Consistent), attribute.Int64("tb.conflicts", out.Conflicts))
	return out, nil
}

func fillPercentiles(r *tbv1.ContendRound, lat []int64) {
	if len(lat) == 0 {
		return
	}
	slices.Sort(lat)
	q := func(p float64) int64 { return lat[min(len(lat)-1, int(p*float64(len(lat))))] }
	var sum int64
	for _, v := range lat {
		sum += v
	}
	r.P50Ns, r.P95Ns, r.P99Ns, r.MaxNs = q(0.50), q(0.95), q(0.99), lat[len(lat)-1]
	r.MeanNs = sum / int64(len(lat))
}
