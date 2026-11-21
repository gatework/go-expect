// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"io"
	"os"
	"slices"
)

// Interact relays through the sole Session receiver. It owns input on successful
// admission; input.Close must interrupt Read. Output is borrowed and redacted.
// Escape/EOF cancels and joins the input worker. In-flight write cancellation
// retains the ordinary Send retirement contract. Remote match tails are bounded
// by a streaming window of min(64 KiB, buffer limit), not arbitrary regexp prefixes.
func (s *Session) Interact(ctx context.Context, input io.ReadCloser, output io.Writer, opts ...InteractOption) (err error) {
	o, w, err := s.prepareInteraction(ctx, input, output, opts)
	defer func() { s.notifyInteract(ctx, w, err) }()
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
	o := interactOptions{escape: 0x1d, outputForward: true}
	for _, opt := range opts {
		if opt == nil {
			return interactOptions{}, nil, &ConfigError{Option: "interact", Reason: "nil option"}
		}
		if err := opt(&o); err != nil {
			return interactOptions{}, nil, err
		}
	}
	cases := append(slices.Clone(o.cases), Case{Pattern: EOF()})
	expectOpts := append([]MatchOption{WithTimeout(NoTimeout)}, o.expect...)
	expectOpts = append(expectOpts, WithStreamingFallback(func(context.Context, *Session, Result) (Action, error) { return ContinueKeepTimer, nil }))
	w := waiter{session: s, result: Result{CaseIndex: -1, RuleIndex: -1}}
	if err := w.prepare(ctx, cases, expectOpts); err != nil {
		return interactOptions{}, nil, err
	}
	for _, c := range o.inputs {
		if c.Pattern.kind == literalPattern && len(c.Pattern.text) > s.opts.limit {
			return interactOptions{}, nil, &ConfigError{Option: "input case", Reason: "sequence exceeds buffer limit"}
		}
	}
	localCases := append(cloneCases(o.inputs), Case{Pattern: Literal(string([]byte{o.escape}))})
	matcher, err := newInputMatcher(localCases)
	if err != nil {
		return interactOptions{}, nil, err
	}
	o.matcher = matcher
	return o, &w, nil
}

// interactPrepared admits the receiver before any stream or terminal side effect.
func (s *Session) interactPrepared(ctx context.Context, input io.ReadCloser, output io.Writer, o interactOptions, w *waiter) (err error) {
	if o.cooked {
		return &ConfigError{Option: "cooked callbacks", Reason: "requires InteractTerminal"}
	}
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
	input = &interactionInput{ReadCloser: input}
	w.callbackMode = o.callbackMode
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
	// Install cleanup before invoking any borrowed writer, which may panic.
	installed := false
	defer func() {
		if installed {
			s.clearInteractSink()
		}
	}()
	if initialErr := s.installInteractSink(output, o.outputForward, &installed); initialErr != nil {
		return errors.Join(initialErr, s.Close())
	}
	if !o.outputForward {
		if e := s.forwardInteraction(w); e != nil {
			return e
		}
	}
	localDone := make(chan error, 1)
	go func() { localDone <- relayInput(ctx, s, input, o, controls, w); cancel() }()
	defer func() {
		cancel()
		closeErr := input.Close()
		inputClosed = true
		s.endInteraction(&err, <-localDone, closeErr)
	}()
	_, remoteErr := w.run(ctx)
	//nolint:errorlint // Do not erase joined causes or WriteError progress.
	if ctx.Err() != nil && remoteErr == context.Canceled {
		remoteErr = nil
	}
	return errors.Join(parent.Err(), remoteErr)
}

// installInteractSink routes diagnostics to the interaction output and, when
// output is forwarded, writes the already received input to it. It sets
// *installed before that first write so that the caller clears the sink even
// if the borrowed writer fails or panics.
func (s *Session) installInteractSink(output io.Writer, forward bool, installed *bool) error {
	s.diagnostics.mu.Lock()
	defer s.diagnostics.mu.Unlock()
	s.diagnostics.interactSink = output
	s.diagnostics.interactDeferred = !forward
	s.diagnostics.interactOutputErr = nil
	s.diagnostics.interactRedactor = &diagnosticRedactor{}
	s.diagnostics.interactRedactor.add(s.diagnostics.secrets)
	*installed = true
	if forward {
		return s.emitInteractiveLocked(s.receive.pending, false)
	}
	return nil
}

// clearInteractSink detaches the interaction output from diagnostics.
func (s *Session) clearInteractSink() {
	s.diagnostics.mu.Lock()
	defer s.diagnostics.mu.Unlock()
	s.diagnostics.interactSink = nil
	s.diagnostics.interactRedactor = nil
	s.diagnostics.interactDeferred = false
}

// forwardInteraction makes w write unmatched remote bytes (and streamed
// matches) to the interaction output when output is not forwarded wholesale.
func (s *Session) forwardInteraction(w *waiter) error {
	if e := w.enableForwarding(); e != nil {
		return e
	}
	w.deliver = func(result Result, hit selection) error {
		if hit.deadlineEvent {
			return nil
		}
		if !hit.streaming && w.cases[hit.chosen].NoConsume {
			return nil
		}
		data := result.Before
		if hit.streaming {
			data += result.Match
		}
		return s.interactiveWrite([]byte(data), false)
	}
	return nil
}

// endInteraction flushes and detaches the interaction output after the local
// relay ended, and joins the local, input-close and flush failures into *err.
// Closing an already closed input is not a failure.
func (s *Session) endInteraction(err *error, localErr, closeErr error) {
	s.diagnostics.mu.Lock()
	flushErr := func() error { defer s.diagnostics.mu.Unlock(); return s.emitInteractiveLocked(nil, true) }()
	s.clearInteractSink()
	if errors.Is(closeErr, io.ErrClosedPipe) || errors.Is(closeErr, os.ErrClosed) {
		closeErr = nil
	}
	if flushErr != nil {
		flushErr = errors.Join(flushErr, s.Close())
	}
	*err = errors.Join(*err, localErr, closeErr, flushErr)
}

func (s *Session) interactiveWrite(data []byte, finish bool) error {
	s.diagnostics.mu.Lock()
	defer s.diagnostics.mu.Unlock()
	return s.emitInteractiveLocked(data, finish)
}

func (s *Session) notifyInteract(ctx context.Context, w *waiter, err error) {
	r := Result{SessionID: s.id, CaseIndex: -1, RuleIndex: -1}
	if w != nil {
		r = w.result
	}
	s.notify(ctx, Event{SessionID: s.id, Operation: OpInteract, Source: r.Source, Scope: r.Scope, CaseIndex: r.CaseIndex, RuleIndex: r.RuleIndex, Bytes: len(r.Before) + len(r.Match), Class: errorClass(err)})
}
