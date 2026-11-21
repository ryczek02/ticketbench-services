package runner

import (
	"context"
	"net"
	"testing"

	tbv1 "github.com/ryczek02/ticketbench-contracts/gen/ticketbench/v1"
	"github.com/ryczek02/ticketbench-contracts/pkg/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func TestParseStores(t *testing.T) {
	got, err := ParseStores("postgres=a:1, valkey=b:2")
	if err != nil || len(got) != 2 || got["valkey"] != "b:2" {
		t.Fatalf("got %v, %v", got, err)
	}
	for _, bad := range []string{"", "postgres", "=x:1", "a="} {
		if _, err := ParseStores(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

// fakeStore records the traceparent it receives.
type fakeStore struct {
	tbv1.UnimplementedStoreServiceServer
	traceparents []string
}

func (f *fakeStore) Execute(ctx context.Context, req *tbv1.ExecuteRequest) (*tbv1.ExecuteResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	f.traceparents = append(f.traceparents, md.Get("traceparent")...)
	return &tbv1.ExecuteResponse{ServerNs: 1000, Rows: 1}, nil
}

func (f *fakeStore) Info(context.Context, *tbv1.InfoRequest) (*tbv1.InfoResponse, error) {
	return &tbv1.InfoResponse{Engine: "fake", Strategies: []string{"a", "b"}}, nil
}

func (f *fakeStore) Contend(_ context.Context, req *tbv1.ContendRequest) (*tbv1.ContendResponse, error) {
	return &tbv1.ContendResponse{Rounds: []*tbv1.ContendRound{{Round: 1, Successes: int64(req.HotTickets), Consistent: true}}}, nil
}

type collect struct {
	grpc.ServerStream
	ctx    context.Context
	events []*tbv1.RunEvent
}

func (c *collect) Send(e *tbv1.RunEvent) error { c.events = append(c.events, e); return nil }
func (c *collect) Context() context.Context    { return c.ctx }

func TestRunPropagatesTrace(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "true")
	shutdown, _ := telemetry.Setup(context.Background(), telemetry.Options{Service: "test", Ratio: 1})
	defer shutdown(context.Background())
	otel.SetTextMapPropagator(propagation.TraceContext{})

	lis := bufconn.Listen(1 << 20)
	fake := &fakeStore{}
	g := grpc.NewServer(telemetry.GRPCServerOptions()...)
	tbv1.RegisterStoreServiceServer(g, fake)
	go g.Serve(lis)
	defer g.Stop()
	conn, err := grpc.NewClient("passthrough:///buf", append(telemetry.GRPCDialOptions(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(map[string]*Store{"fake": {Name: "fake", Client: tbv1.NewStoreServiceClient(conn), conn: conn}})

	ctx, span := telemetry.Tracer("test").Start(context.Background(), "cli")
	defer span.End()
	traceID := span.SpanContext().TraceID().String()
	stream := &collect{ctx: ctx}
	req := &tbv1.RunRequest{RunId: "r1", Steps: []*tbv1.Step{
		{Id: "s1", Store: "fake", Dataset: &tbv1.DatasetSpec{Tickets: 5000, Seed: 1},
			Kind: &tbv1.Step_Scenarios{Scenarios: &tbv1.ScenarioStep{Scenarios: []string{"R1"}, Repetitions: 3, Warmup: 1}}},
		{Id: "s2", Store: "fake", Kind: &tbv1.Step_Contend{Contend: &tbv1.ContendStep{Clients: []int32{1, 10}, HotTickets: 5, Rounds: 1, Strategies: []string{"all"}}}},
	}}
	if err := srv.Run(req, stream); err != nil {
		t.Fatal(err)
	}
	var scen, cont int
	for _, e := range stream.events {
		switch ev := e.Event.(type) {
		case *tbv1.RunEvent_Scenario:
			scen++
			if ev.Scenario.Meta.TraceId != traceID {
				t.Errorf("result trace id %s, want %s", ev.Scenario.Meta.TraceId, traceID)
			}
		case *tbv1.RunEvent_Contend:
			cont++
		case *tbv1.RunEvent_Error:
			t.Fatalf("step error: %s", ev.Error.Message)
		}
	}
	if scen != 4 {
		t.Errorf("%d scenario rows, want 4 (1 warmup + 3)", scen)
	}
	if cont != 4 {
		t.Errorf("%d contention rows, want 4 (2 strategies x 2 client counts)", cont)
	}
	if len(fake.traceparents) != 4 {
		t.Fatalf("store saw %d traceparent headers", len(fake.traceparents))
	}
	for _, tp := range fake.traceparents {
		if len(tp) < 36 || tp[3:35] != traceID {
			t.Errorf("traceparent %q does not carry trace %s", tp, traceID)
		}
	}
}
