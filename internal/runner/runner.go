// Package runner implements RunnerService: it executes a benchmark plan step
// by step against the store services and streams result rows back.
package runner

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	tbv1 "github.com/ryczek02/ticketbench-contracts/gen/ticketbench/v1"
	"github.com/ryczek02/ticketbench-contracts/pkg/telemetry"
	"github.com/ryczek02/ticketbench-contracts/pkg/version"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Store is one configured store service.
type Store struct {
	Name    string
	Address string
	Client  tbv1.StoreServiceClient
	conn    *grpc.ClientConn
}

// ParseStores parses "postgres=store-postgres:7001,valkey=store-valkey:7004".
func ParseStores(spec string) (map[string]string, error) {
	out := map[string]string{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, addr, ok := strings.Cut(part, "=")
		if !ok || name == "" || addr == "" {
			return nil, fmt.Errorf("bad store entry %q, want name=host:port", part)
		}
		out[name] = addr
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no stores configured")
	}
	return out, nil
}

// Dial connects to every store. Connections are lazy, so a store that is
// down only fails the steps that use it.
func Dial(stores map[string]string) (map[string]*Store, error) {
	out := map[string]*Store{}
	for name, addr := range stores {
		opts := append(telemetry.GRPCDialOptions(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)),
		)
		conn, err := grpc.NewClient(addr, opts...)
		if err != nil {
			return nil, err
		}
		out[name] = &Store{Name: name, Address: addr, Client: tbv1.NewStoreServiceClient(conn), conn: conn}
	}
	return out, nil
}

// Server implements tbv1.RunnerServiceServer.
type Server struct {
	tbv1.UnimplementedRunnerServiceServer
	stores map[string]*Store
	tracer trace.Tracer

	mu      sync.Mutex
	indexes map[string]bool
}

func NewServer(stores map[string]*Store) *Server {
	return &Server{stores: stores, tracer: telemetry.Tracer("ticketbench/runner"), indexes: map[string]bool{}}
}

func (s *Server) Close() {
	for _, st := range s.stores {
		_ = st.conn.Close()
	}
}

func (s *Server) Ping(ctx context.Context, _ *tbv1.PingRequest) (*tbv1.PingResponse, error) {
	names := make([]string, 0, len(s.stores))
	for n := range s.stores {
		names = append(names, n)
	}
	sort.Strings(names)
	resp := &tbv1.PingResponse{RunnerVersion: version.String()}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, n := range names {
		st := s.stores[n]
		status := &tbv1.StoreStatus{Name: n, Address: st.Address}
		resp.Stores = append(resp.Stores, status)
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			info, err := st.Client.Info(cctx, &tbv1.InfoRequest{})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				status.Error = err.Error()
				return
			}
			status.Healthy = true
			status.Engine, status.ServerVersion, status.BuildVersion, status.Strategies = info.Engine, info.ServerVersion, info.BuildVersion, info.Strategies
		}()
	}
	wg.Wait()
	return resp, nil
}

type stepRun struct {
	s      *Server
	req    *tbv1.RunRequest
	step   *tbv1.Step
	store  *Store
	stream grpc.ServerStreamingServer[tbv1.RunEvent]
}

func (r *stepRun) meta(ctx context.Context) *tbv1.ResultMeta {
	r.s.mu.Lock()
	idx := r.s.indexes[r.step.Store]
	r.s.mu.Unlock()
	return &tbv1.ResultMeta{
		RunId:   r.req.RunId,
		StepId:  r.step.Id,
		Store:   r.step.Store,
		Tickets: r.step.GetDataset().GetTickets(),
		Indexes: idx,
		TraceId: telemetry.TraceID(ctx),
		Labels:  r.req.Labels,
	}
}

func (r *stepRun) send(ev *tbv1.RunEvent) error { return r.stream.Send(ev) }

func (s *Server) Run(req *tbv1.RunRequest, stream grpc.ServerStreamingServer[tbv1.RunEvent]) error {
	ctx, span := s.tracer.Start(stream.Context(), "runner.run", trace.WithAttributes(
		attribute.String("tb.run_id", req.RunId), attribute.Int("tb.steps", len(req.Steps))))
	defer span.End()
	slog.InfoContext(ctx, "run started", "run_id", req.RunId, "steps", len(req.Steps))

	for i, step := range req.Steps {
		st, ok := s.stores[step.Store]
		if !ok {
			return status.Errorf(codes.InvalidArgument, "step %s: unknown store %q", step.Id, step.Store)
		}
		r := &stepRun{s: s, req: req, step: step, store: st, stream: stream}
		if err := r.send(&tbv1.RunEvent{Event: &tbv1.RunEvent_Progress{Progress: &tbv1.Progress{
			StepId: step.Id, Store: step.Store, StepIndex: int32(i + 1), StepCount: int32(len(req.Steps)),
			Message: describe(step),
		}}}); err != nil {
			return err
		}
		sctx, sspan := s.tracer.Start(ctx, "step "+step.Id, trace.WithAttributes(
			attribute.String("tb.store", step.Store), attribute.String("tb.kind", kind(step))))
		err := r.exec(sctx)
		if err != nil {
			sspan.RecordError(err)
			sspan.SetStatus(otelcodes.Error, err.Error())
			slog.ErrorContext(sctx, "step failed", "step", step.Id, "err", err)
			if serr := r.send(&tbv1.RunEvent{Event: &tbv1.RunEvent_Error{Error: &tbv1.StepError{Meta: r.meta(sctx), Message: err.Error()}}}); serr != nil {
				sspan.End()
				return serr
			}
		}
		sspan.End()
		if err != nil && req.FailFast {
			return status.Errorf(codes.Aborted, "step %s failed: %v", step.Id, err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	slog.InfoContext(ctx, "run finished", "run_id", req.RunId)
	return nil
}

func kind(s *tbv1.Step) string {
	switch s.Kind.(type) {
	case *tbv1.Step_Reset_:
		return "reset"
	case *tbv1.Step_Load:
		return "load"
	case *tbv1.Step_Indexes:
		return "indexes"
	case *tbv1.Step_Scenarios:
		return "scenarios"
	case *tbv1.Step_Explain:
		return "explain"
	case *tbv1.Step_Contend:
		return "contend"
	case *tbv1.Step_Stats:
		return "stats"
	}
	return "unknown"
}

func describe(s *tbv1.Step) string {
	t := s.GetDataset().GetTickets()
	switch k := s.Kind.(type) {
	case *tbv1.Step_Load:
		return fmt.Sprintf("load %d tickets", t)
	case *tbv1.Step_Indexes:
		if k.Indexes.Enabled {
			return "create indexes"
		}
		return "drop indexes"
	case *tbv1.Step_Scenarios:
		return fmt.Sprintf("%d scenarios x %d reps", len(k.Scenarios.Scenarios), k.Scenarios.Repetitions)
	case *tbv1.Step_Contend:
		return fmt.Sprintf("contention clients=%v strategies=%v", k.Contend.Clients, k.Contend.Strategies)
	}
	return kind(s)
}

func (r *stepRun) exec(ctx context.Context) error {
	c := r.store.Client
	ds := r.step.Dataset
	switch k := r.step.Kind.(type) {
	case *tbv1.Step_Reset_:
		res, err := c.Reset(ctx, &tbv1.ResetRequest{})
		if err != nil {
			return err
		}
		r.s.setIndexes(r.step.Store, false)
		return r.send(load(r.meta(ctx), "reset", res.ServerNs, nil, ""))
	case *tbv1.Step_Load:
		res, err := c.Load(ctx, &tbv1.LoadRequest{Dataset: ds, BatchSize: k.Load.BatchSize, Workers: k.Load.Workers})
		if err != nil {
			return err
		}
		return r.send(load(r.meta(ctx), "load", res.ServerNs, res.Rows, res.Checksum))
	case *tbv1.Step_Indexes:
		res, err := c.SetIndexes(ctx, &tbv1.SetIndexesRequest{Enabled: k.Indexes.Enabled})
		if err != nil {
			return err
		}
		r.s.setIndexes(r.step.Store, k.Indexes.Enabled)
		name := "indexes_off"
		if k.Indexes.Enabled {
			name = "indexes_on"
		}
		return r.send(load(r.meta(ctx), name, res.ServerNs, nil, ""))
	case *tbv1.Step_Scenarios:
		return r.scenarios(ctx, k.Scenarios)
	case *tbv1.Step_Explain:
		for _, sc := range k.Explain.Scenarios {
			res, err := c.Explain(ctx, &tbv1.ExplainRequest{Scenario: sc, Iteration: k.Explain.Iteration, Dataset: ds})
			if err != nil {
				return fmt.Errorf("explain %s: %w", sc, err)
			}
			if err := r.send(&tbv1.RunEvent{Event: &tbv1.RunEvent_Plan{Plan: &tbv1.PlanResult{
				Meta: r.meta(ctx), Scenario: sc, Format: res.Format, Plan: res.Plan, Statement: res.Statement,
			}}}); err != nil {
				return err
			}
		}
		return nil
	case *tbv1.Step_Contend:
		return r.contend(ctx, k.Contend)
	case *tbv1.Step_Stats:
		res, err := c.Stats(ctx, &tbv1.StatsRequest{})
		if err != nil {
			return err
		}
		return r.send(&tbv1.RunEvent{Event: &tbv1.RunEvent_Stats{Stats: &tbv1.StatsResult{
			Meta: r.meta(ctx), DataBytes: res.DataBytes, IndexBytes: res.IndexBytes, TotalBytes: res.TotalBytes, Rows: res.Rows,
		}}})
	}
	return fmt.Errorf("step %s has no kind", r.step.Id)
}

func load(m *tbv1.ResultMeta, kind string, ns int64, rows map[string]int64, sum string) *tbv1.RunEvent {
	return &tbv1.RunEvent{Event: &tbv1.RunEvent_Load{Load: &tbv1.LoadResult{Meta: m, Kind: kind, ServerNs: ns, Rows: rows, Checksum: sum}}}
}

func (s *Server) setIndexes(store string, on bool) {
	s.mu.Lock()
	s.indexes[store] = on
	s.mu.Unlock()
}

func (r *stepRun) scenarios(ctx context.Context, sc *tbv1.ScenarioStep) error {
	reps := max(1, sc.Repetitions)
	total := sc.Warmup + reps
	for _, id := range sc.Scenarios {
		for rep := int32(0); rep < total; rep++ {
			it := sc.IterationBase + int64(rep)
			ectx, span := r.s.tracer.Start(ctx, "execute "+id, trace.WithAttributes(attribute.Int64("tb.iteration", it)))
			start := time.Now()
			res, err := r.store.Client.Execute(ectx, &tbv1.ExecuteRequest{Scenario: id, Iteration: it, Dataset: r.step.Dataset, TimeoutMs: sc.TimeoutMs})
			e2e := time.Since(start)
			out := &tbv1.ScenarioResult{
				Meta: r.meta(ectx), Scenario: id, Repetition: rep - sc.Warmup, Warmup: rep < sc.Warmup, E2ENs: e2e.Nanoseconds(),
			}
			if err != nil {
				out.Error = err.Error()
				span.RecordError(err)
				span.SetStatus(otelcodes.Error, err.Error())
			} else {
				out.ServerNs, out.PrepareNs, out.Rows, out.TimedOut = res.ServerNs, res.PrepareNs, res.Rows, res.TimedOut
			}
			span.End()
			if err := r.send(&tbv1.RunEvent{Event: &tbv1.RunEvent_Scenario{Scenario: out}}); err != nil {
				return err
			}
			// A timeout will not get better on the next repetition.
			if out.TimedOut || out.Error != "" {
				break
			}
		}
	}
	return nil
}

func (r *stepRun) contend(ctx context.Context, cs *tbv1.ContendStep) error {
	strategies := cs.Strategies
	if len(strategies) == 0 || (len(strategies) == 1 && strategies[0] == "all") {
		info, err := r.store.Client.Info(ctx, &tbv1.InfoRequest{})
		if err != nil {
			return err
		}
		if len(strategies) == 0 {
			strategies = info.Strategies[:1]
		} else {
			strategies = info.Strategies
		}
	}
	for _, strategy := range strategies {
		for _, clients := range cs.Clients {
			res, err := r.store.Client.Contend(ctx, &tbv1.ContendRequest{
				Dataset: r.step.Dataset, Clients: clients, HotTickets: cs.HotTickets, Rounds: cs.Rounds,
				Strategy: strategy, TraceRatio: cs.TraceRatio,
			})
			if err != nil {
				return fmt.Errorf("contend %s/%d: %w", strategy, clients, err)
			}
			for _, round := range res.Rounds {
				if err := r.send(&tbv1.RunEvent{Event: &tbv1.RunEvent_Contend{Contend: &tbv1.ContendResult{
					Meta: r.meta(ctx), Clients: clients, HotTickets: cs.HotTickets, Strategy: strategy, Round: round,
				}}}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
