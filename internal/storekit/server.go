package storekit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	tbv1 "github.com/ryczek02/ticketbench-contracts/gen/ticketbench/v1"
	"github.com/ryczek02/ticketbench-contracts/pkg/datagen"
	"github.com/ryczek02/ticketbench-contracts/pkg/scenario"
	"github.com/ryczek02/ticketbench-contracts/pkg/telemetry"
	"github.com/ryczek02/ticketbench-contracts/pkg/version"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server adapts an Engine to tbv1.StoreServiceServer.
type Server struct {
	tbv1.UnimplementedStoreServiceServer

	eng    Engine
	prof   *profiles
	tracer trace.Tracer

	scenarioHist metric.Float64Histogram
	attemptHist  metric.Float64Histogram
	conflicts    metric.Int64Counter

	mu       sync.Mutex
	datasets map[datagen.Spec]*datagen.Dataset
	indexes  bool
	// busy serializes operations that change the whole dataset.
	busy sync.Mutex
}

// NewServer wraps eng.
func NewServer(eng Engine) *Server {
	m := telemetry.Meter("ticketbench/store")
	s := &Server{eng: eng, tracer: telemetry.Tracer("ticketbench/store"), datasets: map[datagen.Spec]*datagen.Dataset{}}
	s.scenarioHist, _ = m.Float64Histogram("tb.store.scenario.duration", metric.WithUnit("ms"),
		metric.WithDescription("Server-side duration of one scenario iteration"),
		metric.WithExplicitBucketBoundaries(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000))
	s.attemptHist, _ = m.Float64Histogram("tb.store.contention.attempt", metric.WithUnit("ms"),
		metric.WithDescription("Duration of one booking attempt in the contention test"),
		metric.WithExplicitBucketBoundaries(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 5000))
	s.conflicts, _ = m.Int64Counter("tb.store.contention.conflicts", metric.WithDescription("Booking attempts that lost the seat"))
	return s
}

func (s *Server) dataset(spec *tbv1.DatasetSpec) (*datagen.Dataset, error) {
	if spec == nil {
		return nil, status.Error(codes.InvalidArgument, "dataset is required")
	}
	key := datagen.Spec{Tickets: spec.GetTickets(), Seed: spec.GetSeed()}
	s.mu.Lock()
	defer s.mu.Unlock()
	if d, ok := s.datasets[key]; ok {
		return d, nil
	}
	d, err := datagen.New(key)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.datasets[key] = d
	return d, nil
}

func (s *Server) attrs(extra ...attribute.KeyValue) []attribute.KeyValue {
	return append([]attribute.KeyValue{
		attribute.String("db.system", s.eng.Name()),
		attribute.Bool("tb.indexes", s.indexesOn()),
	}, extra...)
}

func (s *Server) indexesOn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.indexes
}

func fail(span trace.Span, err error) error {
	span.RecordError(err)
	span.SetStatus(otelcodes.Error, err.Error())
	if _, ok := status.FromError(err); ok && status.Code(err) != codes.Unknown {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func (s *Server) Info(ctx context.Context, _ *tbv1.InfoRequest) (*tbv1.InfoResponse, error) {
	v, err := s.eng.ServerVersion(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &tbv1.InfoResponse{Engine: s.eng.Name(), ServerVersion: v, BuildVersion: version.String(), Strategies: s.eng.Strategies(), Profiles: s.profileNames()}, nil
}

func (s *Server) Reset(ctx context.Context, _ *tbv1.ResetRequest) (*tbv1.ResetResponse, error) {
	s.busy.Lock()
	defer s.busy.Unlock()
	ctx, span := s.tracer.Start(ctx, "store.reset", trace.WithAttributes(s.attrs()...))
	defer span.End()
	start := time.Now()
	if err := s.eng.Reset(ctx); err != nil {
		return nil, fail(span, err)
	}
	s.mu.Lock()
	s.indexes = false
	s.mu.Unlock()
	return &tbv1.ResetResponse{ServerNs: time.Since(start).Nanoseconds()}, nil
}

func (s *Server) Load(ctx context.Context, req *tbv1.LoadRequest) (*tbv1.LoadResponse, error) {
	d, err := s.dataset(req.GetDataset())
	if err != nil {
		return nil, err
	}
	s.busy.Lock()
	defer s.busy.Unlock()
	ctx, span := s.tracer.Start(ctx, "store.load", trace.WithAttributes(s.attrs(attribute.Int64("tb.tickets", d.Spec.Tickets))...))
	defer span.End()
	opts := LoadOptions{BatchSize: int(req.GetBatchSize()), Workers: int(req.GetWorkers())}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 5000
	}
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	// The checksum only needs CPU, compute it while the database is busy.
	sum := make(chan string, 1)
	go func() { sum <- d.Checksum() }()
	slog.InfoContext(ctx, "load started", "tickets", d.Spec.Tickets, "workers", opts.Workers)
	start := time.Now()
	rows, err := s.eng.Load(telemetry.Quiet(ctx), d, opts)
	if err != nil {
		return nil, fail(span, err)
	}
	took := time.Since(start)
	slog.InfoContext(ctx, "load finished", "took", took.String(), "rows", rows)
	return &tbv1.LoadResponse{ServerNs: took.Nanoseconds(), Rows: rows, Checksum: <-sum}, nil
}

func (s *Server) SetIndexes(ctx context.Context, req *tbv1.SetIndexesRequest) (*tbv1.SetIndexesResponse, error) {
	s.busy.Lock()
	defer s.busy.Unlock()
	ctx, span := s.tracer.Start(ctx, "store.indexes", trace.WithAttributes(s.attrs(attribute.Bool("tb.enable", req.GetEnabled()))...))
	defer span.End()
	start := time.Now()
	if err := s.eng.SetIndexes(ctx, req.GetEnabled()); err != nil {
		return nil, fail(span, err)
	}
	s.mu.Lock()
	s.indexes = req.GetEnabled()
	s.mu.Unlock()
	return &tbv1.SetIndexesResponse{ServerNs: time.Since(start).Nanoseconds()}, nil
}

func (s *Server) params(req interface {
	GetScenario() string
	GetIteration() int64
	GetDataset() *tbv1.DatasetSpec
}) (scenario.Params, error) {
	d, err := s.dataset(req.GetDataset())
	if err != nil {
		return scenario.Params{}, err
	}
	id := scenario.ID(req.GetScenario())
	if _, ok := scenario.Lookup(id); !ok {
		return scenario.Params{}, status.Errorf(codes.InvalidArgument, "unknown scenario %q", id)
	}
	p, err := scenario.For(d, id, req.GetIteration())
	if err != nil {
		return scenario.Params{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return p, nil
}

func (s *Server) Execute(ctx context.Context, req *tbv1.ExecuteRequest) (*tbv1.ExecuteResponse, error) {
	p, err := s.params(req)
	if err != nil {
		return nil, err
	}
	eng, err := s.engine(ctx, req.GetProfile())
	if err != nil {
		return nil, err
	}
	ctx, span := s.tracer.Start(ctx, "scenario "+string(p.ID), trace.WithAttributes(s.attrs(
		attribute.String("tb.scenario", string(p.ID)),
		attribute.Int64("tb.iteration", p.Iteration),
		attribute.String("tb.profile", req.GetProfile()),
	)...))
	defer span.End()

	var prepare time.Duration
	if def, _ := scenario.Lookup(p.ID); def.Fixture || !p.Fixture.Empty() {
		pctx, pspan := s.tracer.Start(ctx, "prepare")
		start := time.Now()
		err := eng.Prepare(pctx, p)
		prepare = time.Since(start)
		pspan.End()
		if err != nil {
			return nil, fail(span, fmt.Errorf("prepare: %w", err))
		}
	}

	rctx := ctx
	if ms := req.GetTimeoutMs(); ms > 0 {
		var cancel context.CancelFunc
		rctx, cancel = context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
		defer cancel()
	}
	start := time.Now()
	rows, err := eng.Run(rctx, p)
	took := time.Since(start)
	timedOut := errors.Is(err, context.DeadlineExceeded) || (err != nil && rctx.Err() != nil)
	if err != nil && !timedOut {
		return nil, fail(span, err)
	}
	span.SetAttributes(attribute.Int64("tb.rows", rows), attribute.Bool("tb.timed_out", timedOut))
	s.scenarioHist.Record(ctx, float64(took.Microseconds())/1000, metric.WithAttributes(s.attrs(attribute.String("tb.scenario", string(p.ID)), attribute.String("tb.profile", req.GetProfile()))...))
	return &tbv1.ExecuteResponse{ServerNs: took.Nanoseconds(), PrepareNs: prepare.Nanoseconds(), Rows: rows, TimedOut: timedOut}, nil
}

func (s *Server) Explain(ctx context.Context, req *tbv1.ExplainRequest) (*tbv1.ExplainResponse, error) {
	p, err := s.params(req)
	if err != nil {
		return nil, err
	}
	ctx, span := s.tracer.Start(ctx, "explain "+string(p.ID), trace.WithAttributes(s.attrs(attribute.String("tb.scenario", string(p.ID)))...))
	defer span.End()
	fixture := !p.Fixture.Empty()
	if fixture {
		if err := s.eng.Prepare(ctx, p); err != nil {
			return nil, fail(span, fmt.Errorf("prepare: %w", err))
		}
	}
	plan, err := s.eng.Explain(ctx, p)
	if err != nil {
		return nil, fail(span, err)
	}
	if fixture {
		// Consume the fixture so it cannot leak into later iterations
		// (D3 would otherwise delete two batches instead of one).
		if _, err := s.eng.Run(ctx, p); err != nil {
			return nil, fail(span, fmt.Errorf("cleanup after explain: %w", err))
		}
	}
	return &tbv1.ExplainResponse{Format: plan.Format, Plan: plan.Plan, Statement: plan.Statement}, nil
}

func (s *Server) Stats(ctx context.Context, _ *tbv1.StatsRequest) (*tbv1.StatsResponse, error) {
	ctx, span := s.tracer.Start(ctx, "store.stats", trace.WithAttributes(s.attrs()...))
	defer span.End()
	st, err := s.eng.Stats(ctx)
	if err != nil {
		return nil, fail(span, err)
	}
	return &tbv1.StatsResponse{DataBytes: st.DataBytes, IndexBytes: st.IndexBytes, TotalBytes: st.TotalBytes, Rows: st.Rows}, nil
}

func (s *Server) strategy(name string) (string, error) {
	all := s.eng.Strategies()
	if name == "" || name == "default" {
		return all[0], nil
	}
	if !slices.Contains(all, name) {
		return "", status.Errorf(codes.InvalidArgument, "strategy %q not supported by %s (have %v)", name, s.eng.Name(), all)
	}
	return name, nil
}
