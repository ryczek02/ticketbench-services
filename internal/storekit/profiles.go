package storekit

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ProfileOpener connects with the settings of a named connection profile,
// e.g. "role" (least-privilege application user) or "tls".
type ProfileOpener func(ctx context.Context, profile string) (Engine, error)

// profiles keeps one lazily opened engine per connection profile. Data
// loading and schema changes always use the default engine (admin user);
// profiles only run scenarios and the contention test.
type profiles struct {
	open    ProfileOpener
	allowed []string

	mu      sync.Mutex
	engines map[string]Engine
}

// WithProfiles enables the given connection profiles on the server.
func (s *Server) WithProfiles(open ProfileOpener, names []string) *Server {
	s.prof = &profiles{open: open, allowed: names, engines: map[string]Engine{}}
	return s
}

// ProfileNames parses "role,tls,tls_role".
func ProfileNames(spec string) []string {
	var out []string
	for _, p := range strings.Split(spec, ",") {
		if p = strings.TrimSpace(p); p != "" && p != "base" {
			out = append(out, p)
		}
	}
	return out
}

// ProfileEnvSuffix maps "tls_role" to "_TLS_ROLE" for environment variables.
func ProfileEnvSuffix(profile string) string {
	if profile == "" {
		return ""
	}
	return "_" + strings.ToUpper(profile)
}

func (s *Server) profileNames() []string {
	if s.prof == nil {
		return nil
	}
	return s.prof.allowed
}

// engine returns the engine for a profile; "" and "base" are the default.
func (s *Server) engine(ctx context.Context, profile string) (Engine, error) {
	if profile == "" || profile == "base" {
		return s.eng, nil
	}
	if s.prof == nil || !slices.Contains(s.prof.allowed, profile) {
		return nil, status.Errorf(codes.InvalidArgument, "profile %q is not configured on %s", profile, s.eng.Name())
	}
	s.prof.mu.Lock()
	defer s.prof.mu.Unlock()
	if e, ok := s.prof.engines[profile]; ok {
		return e, nil
	}
	e, err := s.prof.open(ctx, profile)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "open profile %s: %v", profile, err)
	}
	s.prof.engines[profile] = e
	return e, nil
}

// CloseProfiles closes engines opened for profiles.
func (s *Server) CloseProfiles() {
	if s.prof == nil {
		return
	}
	s.prof.mu.Lock()
	defer s.prof.mu.Unlock()
	for name, e := range s.prof.engines {
		if err := e.Close(); err != nil {
			fmt.Printf("close profile %s: %v\n", name, err)
		}
	}
}
