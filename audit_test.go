package expect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"testing"
)

// Cancellation occurs inside Write, before the write result can become ready.
func TestSendCancellationBeforeWriteCompletion(t *testing.T) {
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(t.Context())
		transport := &cancelingWriter{cancel: cancel}
		s, err := NewSession(transport)
		if err != nil {
			t.Fatal(err)
		}
		n, err := s.Send(ctx, "yes")
		closed := s.isClosed()
		s.Close()
		cancel()
		var we *WriteError
		if n != 3 || !errors.As(err, &we) || !errors.Is(err, context.Canceled) || !closed {
			t.Fatalf("cancellation before completion lost: n=%d err=%v closed=%v", n, err, closed)
		}
	}
}

type cancelingWriter struct{ cancel context.CancelFunc }

func (*cancelingWriter) Read([]byte) (int, error)      { return 0, io.EOF }
func (w *cancelingWriter) Write(p []byte) (int, error) { w.cancel(); return len(p), nil }
func (*cancelingWriter) Close() error                  { return nil }

func TestSendCloseBeforeWriteCompletion(t *testing.T) {
	for i := 0; i < 100; i++ {
		transport := &cancelingWriter{}
		s, err := NewSession(transport)
		if err != nil {
			t.Fatal(err)
		}
		transport.cancel = func() {
			go s.Close()
			<-s.done
		}
		n, err := s.Send(t.Context(), "yes")
		s.Close()
		var we *WriteError
		if n != 3 || !errors.As(err, &we) || !errors.Is(err, ErrClosed) {
			t.Fatalf("close before completion lost: n=%d err=%v", n, err)
		}
	}
}

// Short reads, full blocks and trailing fragments cross the ownership threshold.
func TestReceiveScratchDoesNotChangeQueuedInput(t *testing.T) {
	for _, chunk := range []int{1, 32, readChunk/2 - 1, readChunk / 2, readChunk} {
		t.Run(strconv.Itoa(chunk), func(t *testing.T) {
			data := make([]byte, readChunk*3+37)
			for i := range data {
				data[i] = byte(i * 37)
			}
			var transcript bytes.Buffer
			s, err := NewSession(&chunkStream{Reader: bytes.NewReader(data), chunk: chunk}, WithTranscript(&transcript))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			var snapshots []Result
			count := 0
			for _, value := range data {
				if value == 255 {
					count++
				}
			}
			last, err := s.Expect(t.Context(), []Case{{Pattern: Literal("\xff"), Handler: func(_ context.Context, _ *Session, r Result) (Action, error) {
				snapshots = append(snapshots, r)
				return ContinueWithoutReset, nil
			}}, {Pattern: EOF()}})
			if err != nil || len(snapshots) != count {
				t.Fatalf("matches=%d want=%d err=%v", len(snapshots), count, err)
			}
			var rebuilt bytes.Buffer
			for _, r := range snapshots {
				rebuilt.WriteString(r.Before)
				rebuilt.WriteString(r.Match)
			}
			rebuilt.WriteString(last.Before)
			if !bytes.Equal(rebuilt.Bytes(), data) || !bytes.Equal(transcript.Bytes(), data) {
				t.Fatal("reused scratch changed input or detached snapshots")
			}
		})
	}
}

func TestRejectedWaitReportsPendingInput(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(strconv.FormatBool(canceled), func(t *testing.T) {
			s, peer := pair(t)
			go peer.Write([]byte("promptTAIL"))
			waitText(t, s, "prompt")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var opts []ExpectOption
			if canceled {
				cancel()
			} else {
				opts = []ExpectOption{nil}
			}
			r, err := s.Expect(ctx, []Case{{Pattern: Literal("TAIL")}}, opts...)
			if err == nil || r.CaseIndex != -1 || r.Before != "TAIL" {
				t.Fatalf("rejected wait lost pending diagnostics: err=%v before=%q", err, r.Before)
			}
			waitText(t, s, "TAIL")
		})
	}
}
