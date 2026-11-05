// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
)

// InteractOption configures an owned interactive receive operation.
type InteractOption func(*interactOptions) error
type interactOptions struct {
	cases  []Case
	inputs []Case
	escape byte
	expect []ExpectOption
}

// WithEscape sets the local exit byte; the default is control-] (0x1d).
func WithEscape(key byte) InteractOption {
	return func(o *interactOptions) error { o.escape = key; return nil }
}

// WithInteractInputCases installs ordered, byte-exact local literal actions.
func WithInteractInputCases(cases ...Case) InteractOption {
	cases = append([]Case(nil), cases...)
	return func(o *interactOptions) error {
		if err := validateCases(cases); err != nil {
			return err
		}
		for _, c := range cases {
			if c.Pattern.kind != literalPattern {
				return ErrInvalidPattern
			}
		}
		o.inputs = append(o.inputs, cases...)
		return nil
	}
}

// WithInteractOutputCases adds remote pattern actions; Return ends interaction.
func WithInteractOutputCases(cases ...Case) InteractOption {
	cases = append([]Case(nil), cases...)
	return func(o *interactOptions) error {
		if err := validateCases(cases); err != nil {
			return err
		}
		o.cases = append(o.cases, cases...)
		return nil
	}
}

// WithInteractExpectOptions configures matching and timing during interaction.
func WithInteractExpectOptions(opts ...ExpectOption) InteractOption {
	opts = append([]ExpectOption(nil), opts...)
	return func(o *interactOptions) error { o.expect = append(o.expect, opts...); return nil }
}

// Interact relays through the sole Session receiver. It owns input on successful
// admission; input.Close must interrupt Read. Output is borrowed and redacted.
// Escape/EOF cancels and joins the input worker. In-flight write cancellation
// retains the ordinary Send retirement contract. Remote match tails are bounded
// by a streaming window of min(64 KiB, buffer limit), not arbitrary regexp prefixes.
func (s *Session) Interact(ctx context.Context, input io.ReadCloser, output io.Writer, opts ...InteractOption) (err error) {
	o, w, err := s.prepareInteraction(ctx, input, output, opts)
	if err != nil {
		return err
	}
	return s.interactPrepared(ctx, input, output, o, w)
}

func (s *Session) prepareInteraction(ctx context.Context, input io.ReadCloser, output io.Writer, opts []InteractOption) (interactOptions, *waiter, error) {
	if ctx == nil || nilInterface(input) || nilInterface(output) {
		return interactOptions{}, nil, &ConfigError{Option: "interact", Reason: "context and streams required"}
	}
	if err := ctx.Err(); err != nil {
		return interactOptions{}, nil, err
	}
	o := interactOptions{escape: 0x1d}
	for _, opt := range opts {
		if opt == nil {
			return interactOptions{}, nil, &ConfigError{Option: "interact", Reason: "nil option"}
		}
		if err := opt(&o); err != nil {
			return interactOptions{}, nil, err
		}
	}
	cases := append(append([]Case(nil), o.cases...), Case{Pattern: EOF()})
	expectOpts := append([]ExpectOption{WithTimeout(NoTimeout)}, o.expect...)
	expectOpts = append(expectOpts, WithStreamingFallback(func(context.Context, *Session, Result) (Action, error) { return ContinueKeepTimer, nil }))
	w := waiter{session: s, result: Result{CaseIndex: -1, RuleIndex: -1}}
	if err := w.prepare(ctx, cases, expectOpts); err != nil {
		return interactOptions{}, nil, err
	}
	for _, c := range o.inputs {
		if len(c.Pattern.text) > s.opts.limit {
			return interactOptions{}, nil, &ConfigError{Option: "input case", Reason: "sequence exceeds buffer limit"}
		}
	}
	return o, &w, nil
}

// interactPrepared admits the receiver before any stream or terminal side effect.
func (s *Session) interactPrepared(ctx context.Context, input io.ReadCloser, output io.Writer, o interactOptions, w *waiter) (err error) {
	if !s.expecting.CompareAndSwap(false, true) {
		return ErrBusy
	}
	defer s.releaseWait()
	if s.isClosed() {
		return ErrClosed
	}
	return s.interactOwned(ctx, input, output, o, w)
}

func (s *Session) interactOwned(ctx context.Context, input io.ReadCloser, output io.Writer, o interactOptions, w *waiter) (err error) {
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	controls := make(chan func())
	w.control = controls
	defer w.close()
	inputClosed := false
	defer func() {
		if !inputClosed {
			_ = input.Close()
		}
	}()
	// Install cleanup before invoking any borrowed writer.
	installed := false
	defer func() {
		if installed {
			s.diagnostics.mu.Lock()
			defer s.diagnostics.mu.Unlock()
			s.diagnostics.interactSink = nil
			s.diagnostics.interactRedactor = nil
		}
	}()
	initialErr := func() error {
		s.diagnostics.mu.Lock()
		defer s.diagnostics.mu.Unlock()
		s.diagnostics.interactSink = output
		s.diagnostics.interactOutputErr = nil
		s.diagnostics.interactRedactor = &redactor{}
		s.diagnostics.interactRedactor.add(s.diagnostics.secrets)
		installed = true
		return s.emitInteractiveLocked(s.receive.pending, false)
	}()
	if initialErr != nil {
		return errors.Join(initialErr, s.Close())
	}
	localDone := make(chan error, 1)
	go func() { localDone <- relayInput(ctx, s, input, o.inputs, o.escape, controls, w); cancel() }()
	defer func() {
		cancel()
		closeErr := input.Close()
		inputClosed = true
		localErr := <-localDone
		s.diagnostics.mu.Lock()
		flushErr := func() error { defer s.diagnostics.mu.Unlock(); return s.emitInteractiveLocked(nil, true) }()
		s.diagnostics.mu.Lock()
		s.diagnostics.interactSink = nil
		s.diagnostics.interactRedactor = nil
		s.diagnostics.mu.Unlock()
		if errors.Is(closeErr, io.ErrClosedPipe) || errors.Is(closeErr, os.ErrClosed) {
			closeErr = nil
		}
		if flushErr != nil {
			flushErr = errors.Join(flushErr, s.Close())
		}
		err = errors.Join(err, localErr, closeErr, flushErr)
	}()
	_, remoteErr := w.run(ctx)
	//nolint:errorlint // Do not erase joined causes or WriteError progress.
	if ctx.Err() != nil && remoteErr == context.Canceled {
		remoteErr = nil
	}
	return errors.Join(parent.Err(), remoteErr)
}

// relayInput owns only input I/O; closures execute on the receive owner.
func relayInput(ctx context.Context, s *Session, input io.Reader, cases []Case, escape byte, controls chan<- func(), w *waiter) (err error) {
	pending := make([]byte, 0)
	buf := make([]byte, 4096)
	counts := make([]int, len(cases))
	empty := 0
	actions := inputActions{session: s, waiter: w, cases: cases, counts: counts, controls: controls}
	trie := compileInputTrie(cases)
	flush := func(eof bool) error {
		for len(pending) > 0 {
			matched, partial := trie.match(pending, eof)
			if partial {
				return nil
			}
			if matched >= 0 {
				text := cases[matched].Pattern.text
				if e := actions.invoke(ctx, matched, text); e != nil {
					return e
				}
				pending = pending[len(text):]
				continue
			}
			if pending[0] == escape {
				select {
				case controls <- func() { w.stopped = true }:
				case <-ctx.Done():
				}
				return io.EOF
			}
			n := 1
			for n < len(pending) && pending[n] != escape && trie.edges[pending[n]] == nil {
				n++
			}
			if _, e := s.Send(ctx, string(pending[:n])); e != nil {
				return e
			}
			pending = pending[n:]
		}
		return nil
	}
	for {
		n, e := input.Read(buf)
		if n < 0 || n > len(buf) {
			return errors.Join(errInvalidReadCount, e)
		}
		if n == 0 && e == nil {
			empty++
			if empty >= 100 {
				return io.ErrNoProgress
			}
		} else {
			empty = 0
		}
		if len(cases) == 0 {
			i := bytes.IndexByte(buf[:n], escape)
			end := n
			if i >= 0 {
				end = i
			}
			if end > 0 {
				if _, x := s.Send(ctx, string(buf[:end])); x != nil {
					return x
				}
			}
			if i >= 0 {
				select {
				case controls <- func() { w.stopped = true }:
				case <-ctx.Done():
				}
				return nil
			}
		} else {
			pending = append(pending, buf[:n]...)
			if x := flush(false); x != nil {
				if errors.Is(x, io.EOF) {
					return nil
				}
				return x
			}
		}
		if e != nil {
			if errors.Is(e, io.EOF) {
				x := flush(true)
				if errors.Is(x, io.EOF) {
					return nil
				}
				return x
			}
			//nolint:errorlint // Only exact read errors caused by cleanup are benign.
			if ctx.Err() != nil && (e == io.ErrClosedPipe || e == os.ErrClosed) {
				return nil
			}
			return e
		}
	}
}
