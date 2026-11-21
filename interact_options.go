// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"slices"
	"time"
)

// InteractOption configures an owned interactive receive operation.
type InteractOption func(*interactOptions) error

type interactOptions struct {
	matcher                                   *inputMatcher
	cases                                     []Case
	inputs                                    []Case
	escape                                    byte
	expect                                    []MatchOption
	inputTimeout                              time.Duration
	inputForward, outputForward, echo, cooked bool
	callbackMode                              func() (func() error, error)
}

// WithEscape sets the local exit byte; the default is control-] (0x1d).
func WithEscape(key byte) InteractOption {
	return func(o *interactOptions) error { o.escape = key; return nil }
}

// WithInteractInputCases installs ordered local text patterns and input events.
// Statically empty text matches and NoConsume are rejected before admission.
func WithInteractInputCases(cases ...Case) InteractOption {
	cases = cloneCases(cases)
	return func(o *interactOptions) error {
		if err := validateCases(cases); err != nil {
			return err
		}
		for _, c := range cases {
			switch c.Pattern.kind { //nolint:exhaustive // Event cases need no text validation; every other kind must validate as text.
			case eofPattern, timeoutPattern, defaultPattern, fullBufferPattern:
			default:
				if err := c.Pattern.ValidateText(); err != nil {
					return err
				}
			}
			if c.NoConsume {
				return &ConfigError{Option: "input case", Reason: "use forwarding instead of NoConsume"}
			}
		}

		o.inputs = append(o.inputs, cases...)
		return nil
	}
}

// WithInteractOutputCases adds remote pattern actions; Return ends interaction.
func WithInteractOutputCases(cases ...Case) InteractOption {
	cases = cloneCases(cases)
	return func(o *interactOptions) error {
		if err := validateCases(cases); err != nil {
			return err
		}
		o.cases = append(o.cases, cases...)
		return nil
	}
}

// WithInteractMatchOptions configures matching and timing during interaction.
func WithInteractMatchOptions(opts ...MatchOption) InteractOption {
	opts = slices.Clone(opts)
	return func(o *interactOptions) error { o.expect = append(o.expect, opts...); return nil }
}

// WithInteractInputTimeout sets the local idle interval. Timeout/Default input
// cases receive this event; without a case expiry returns ErrIdleTimeout.
func WithInteractInputTimeout(d time.Duration) InteractOption {
	return func(o *interactOptions) error {
		if d < 0 {
			return &ConfigError{Option: "input timeout", Reason: "must be nonnegative"}
		}
		o.inputTimeout = d
		return nil
	}
}

// WithInteractInputForward forwards matched local sequences before the callback.
func WithInteractInputForward(enabled bool) InteractOption {
	return func(o *interactOptions) error { o.inputForward = enabled; return nil }
}

// WithInteractOutputForward selects immediate remote passthrough (the default).
// Disabling it withholds matched bytes; unmatched bytes use the bounded window.
func WithInteractOutputForward(enabled bool) InteractOption {
	return func(o *interactOptions) error { o.outputForward = enabled; return nil }
}

// WithInteractEcho echoes matched local sequences to the redacted operator output.
func WithInteractEcho(enabled bool) InteractOption {
	return func(o *interactOptions) error { o.echo = enabled; return nil }
}

// WithInteractCookedCallbacks restores the borrowed terminal during callbacks.
// It requires InteractTerminal; ordinary Interact rejects the option.
func WithInteractCookedCallbacks() InteractOption {
	return func(o *interactOptions) error { o.cooked = true; return nil }
}
