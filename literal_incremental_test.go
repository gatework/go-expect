package expect

import (
	"strings"
	"testing"
)

func TestLongLiteralIncrementalAcrossChunks(t *testing.T) {
	for _, p := range []Pattern{Literal(strings.Repeat("a", 4096) + "b"), All(Literal("start"), Literal(strings.Repeat("a", 4096)+"b"))} {
		s := newPatternScan(p)
		if len(s.literals) == 0 {
			t.Fatal("missing incremental cursor")
		}
		data := []byte("start" + strings.Repeat("a", 8192) + "b")
		for end := 1; end <= len(data); end++ {
			got, ok := s.findLiterals(data[:end])
			want, wok, err := p.find(data[:end], false, 0)
			if err != nil || ok != wok || ok && (got.start != want.start || got.end != want.end) {
				t.Fatalf("end=%d got %+v %v want %+v %v", end, got, ok, want, wok)
			}
		}
		s.reset()
		if _, ok := s.findLiterals([]byte("other")); ok {
			t.Fatal("stale state")
		}
	}
}

func BenchmarkLongLiteralCursor(b *testing.B) {
	p := All(Literal(strings.Repeat("a", 4096) + "b"))
	data := []byte(strings.Repeat("a", 16384) + "b")
	s := newPatternScan(p)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		s.reset()
		for end := 32; end < len(data); end += 32 {
			s.findLiterals(data[:end])
		}
		if _, ok := s.findLiterals(data); !ok {
			b.Fatal("missing match")
		}
	}
}
func TestPrivateSafeFilterBorrowsOnlySafeChunk(t *testing.T) {
	var r redactor
	r.add([]string{"secret"})
	data := []byte("xxxx")
	if n := testing.AllocsPerRun(100, func() { r.append(data, false) }); n != 0 {
		t.Fatalf("safe-path allocations: %g", n)
	}
	got := r.append(data, false)
	if &got[0] != &data[0] {
		t.Fatal("safe data copied")
	}
}
