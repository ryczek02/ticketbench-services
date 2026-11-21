package storekit

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type namedEngine struct {
	Engine
	name string
}

func (n namedEngine) Name() string { return n.name }
func (n namedEngine) Close() error { return nil }

func TestProfiles(t *testing.T) {
	opened := map[string]int{}
	s := NewServer(namedEngine{name: "base"}).WithProfiles(func(_ context.Context, p string) (Engine, error) {
		opened[p]++
		return namedEngine{name: p}, nil
	}, ProfileNames("base, role,tls"))

	for _, p := range []string{"", "base"} {
		e, err := s.engine(context.Background(), p)
		if err != nil || e.Name() != "base" {
			t.Fatalf("profile %q: %v %v", p, e, err)
		}
	}
	for range 3 {
		e, err := s.engine(context.Background(), "role")
		if err != nil || e.Name() != "role" {
			t.Fatalf("role: %v %v", e, err)
		}
	}
	if opened["role"] != 1 {
		t.Fatalf("role opened %d times, want 1", opened["role"])
	}
	if _, err := s.engine(context.Background(), "tls_role"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unconfigured profile: %v", err)
	}
	if got := s.profileNames(); len(got) != 2 {
		t.Fatalf("profiles %v (base must not be listed)", got)
	}
	if ProfileEnvSuffix("tls_role") != "_TLS_ROLE" || ProfileEnvSuffix("") != "" {
		t.Fatal("env suffix")
	}
	s.CloseProfiles()
}

func TestTLSConfigFromEnv(t *testing.T) {
	t.Setenv("TB_TLS_CA", "")
	if c, err := TLSConfigFromEnv(); c != nil || err != nil {
		t.Fatal("expected nil config without TB_TLS_CA")
	}
	t.Setenv("TB_TLS_CA", "/does/not/exist")
	if _, err := TLSConfigFromEnv(); err == nil {
		t.Fatal("missing CA file accepted")
	}
}
