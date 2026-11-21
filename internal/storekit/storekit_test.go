package storekit

import (
	"testing"

	tbv1 "github.com/ryczek02/ticketbench-contracts/gen/ticketbench/v1"
)

func TestSQLStatements(t *testing.T) {
	got := SQLStatements("-- comment; with a semicolon\nCREATE TABLE a (x INT);\n\n-- next\nCREATE INDEX i ON a (x);\n")
	if len(got) != 2 || got[0] != "CREATE TABLE a (x INT)" || got[1] != "CREATE INDEX i ON a (x)" {
		t.Fatalf("got %q", got)
	}
}

func TestPercentiles(t *testing.T) {
	lat := make([]int64, 100)
	for i := range lat {
		lat[i] = int64(100 - i)
	}
	var r tbv1.ContendRound
	fillPercentiles(&r, lat)
	if r.P50Ns != 51 || r.P95Ns != 96 || r.P99Ns != 100 || r.MaxNs != 100 || r.MeanNs != 50 {
		t.Fatalf("got p50=%d p95=%d p99=%d max=%d mean=%d", r.P50Ns, r.P95Ns, r.P99Ns, r.MaxNs, r.MeanNs)
	}
	var empty tbv1.ContendRound
	fillPercentiles(&empty, nil)
	if empty.MaxNs != 0 {
		t.Fatal("empty input must leave zeros")
	}
}

func TestChunks(t *testing.T) {
	var spans [][2]int
	_ = Chunks(10, 4, func(a, b int) error { spans = append(spans, [2]int{a, b}); return nil })
	if len(spans) != 3 || spans[2] != [2]int{8, 10} {
		t.Fatalf("got %v", spans)
	}
}

func BenchmarkPercentiles(b *testing.B) {
	lat := make([]int64, 50_000)
	for i := range lat {
		lat[i] = int64((i * 7919) % 50_000)
	}
	buf := make([]int64, len(lat))
	for b.Loop() {
		copy(buf, lat)
		fillPercentiles(&tbv1.ContendRound{}, buf)
	}
}
