// Package connector owns bounded network-device CLI connections and operations.
// It builds on expect without adding device policy to the matching engine.
package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	expect "github.com/gatework/go-expect"
)

var (
	ErrInvalid        = errors.New("connector: invalid input")
	ErrBusy           = errors.New("connector: device busy")
	ErrClosed         = errors.New("connector: device closed")
	ErrNotConnected   = errors.New("connector: device not connected")
	ErrAuthentication = errors.New("connector: authentication failed")
	ErrDevice         = errors.New("connector: device rejected command")
	ErrOutputLimit    = errors.New("connector: output limit exceeded")
	ErrResponseLimit  = errors.New("connector: response limit exceeded")
	ErrUnsupported    = errors.New("connector: unsupported operation")
	ErrIncomplete     = errors.New("connector: incomplete configuration")
	ErrUnconfirmed    = errors.New("connector: save not confirmed")
)

// Error exposes original causes through errors.Is/As, never through formatting.
type Error struct {
	phase string
	cause error
}

func (e *Error) Error() string    { return "connector: " + e.phase + " failed" }
func (e *Error) GoString() string { return e.Error() }
func (e *Error) Unwrap() error    { return e.cause }
func failure(phase string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{phase, err}
}

// Interaction is scoped to one operation. Limit zero means one, never unlimited.
// Respond overrides Reply; callbacks must honor context and must not reenter
// operations on the device. Replies are verbatim, including their line endings.
type Interaction struct {
	Pattern   string
	Reply     string
	Respond   func(context.Context, expect.Result) (string, error)
	Limit     int
	Sensitive bool
	Capture   bool
}

func (Interaction) String() string     { return "connector.Interaction" }
func (i Interaction) GoString() string { return i.String() }

// Command is exactly one CLI line. Prompt declares an explicit mode transition;
// otherwise the complete currently learned prompt is used. Timeout zero inherits.
type Command struct {
	Text         string
	Prompt       string
	Interactions []Interaction
	Timeout      time.Duration
	Sensitive    bool
	Line         int
}

func (Command) String() string                 { return "connector.Command" }
func (c Command) GoString() string             { return c.String() }
func (c Command) MarshalJSON() ([]byte, error) { return json.Marshal(struct{ Line int }{c.Line}) }

// Receipt retains raw sensitive bytes explicitly. Complete means a prompt was
// received, even when the device rejected the command or a later callback failed.
// Written is command-write progress only; it is not proof of device execution.
type Receipt struct {
	Command             string
	Raw, Output, Prompt string
	Written             int
	Complete            bool
	Duration            time.Duration
	Err                 error
}

func (r Receipt) String() string {
	return fmt.Sprintf("connector.Receipt{complete:%t failed:%t}", r.Complete, r.Err != nil)
}
func (r Receipt) GoString() string { return r.String() }
func (r Receipt) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Complete, Failed bool
		Written          int
		Duration         time.Duration
	}{r.Complete, r.Err != nil, r.Written, r.Duration})
}

// Result includes every attempted command, including the final failed attempt.
type Result struct{ Steps []Receipt }

func (r Result) String() string   { return fmt.Sprintf("connector.Result{steps:%d}", len(r.Steps)) }
func (r Result) GoString() string { return r.String() }

// Config is copied by New. Passwords/passphrases never appear in process argv.
// SSH is default; Telnet must be explicitly selected. KnownHosts uses OpenSSH's
// normal path expansion rules. No permissive host-key policy is provided.
type Config struct {
	Host, Username, Password, EnablePassword, KeyPassphrase string
	Protocol                                                string
	Port                                                    int
	KnownHosts, IdentityFile                                string
	SSHPath, TelnetPath                                     string
	LineEnding                                              string
	LoginTimeout, CommandTimeout, WriteTimeout              time.Duration
	MaxOutputBytes, MaxScriptOutputBytes                    int
	// Challenges handles explicit MFA/login prompts before credential rules.
	Challenges []Interaction
}

func (Config) String() string               { return "connector.Config" }
func (c Config) GoString() string           { return c.String() }
func (Config) MarshalJSON() ([]byte, error) { return []byte(`{}`), nil }

// Dialer returns a new owned duplex stream. Close must interrupt Read and Write;
// cancellation must interrupt dialing. A stream returned with an error is closed.
// Config is a value snapshot. The built-in dialer owns an OpenSSH/Telnet process.
type Dialer func(context.Context, Config) (io.ReadWriteCloser, error)
type options struct {
	dial    Dialer
	receipt func(Receipt) error
}
type Option func(*options) error

func WithDialer(dial Dialer) Option {
	return func(o *options) error {
		if dial == nil {
			return ErrInvalid
		}
		o.dial = dial
		return nil
	}
}

// WithReceipt installs a synchronous per-attempt observer. Failure stops the
// script and closes the device; the already completed receipt remains available.
func WithReceipt(fn func(Receipt) error) Option {
	return func(o *options) error {
		if fn == nil {
			return ErrInvalid
		}
		o.receipt = fn
		return nil
	}
}

func normalizeConfig(c Config) (Config, error) {
	if err := validateInteractions(c.Challenges); err != nil {
		return c, err
	}
	c.Challenges = append([]Interaction(nil), c.Challenges...)
	if c.Host == "" || strings.HasPrefix(c.Host, "-") || strings.ContainsAny(c.Host, "\x00\r\n\t /@") || c.Port < 0 || c.Port > 65535 {
		return c, ErrInvalid
	}
	for _, s := range []string{c.Username, c.Password, c.EnablePassword, c.KeyPassphrase, c.KnownHosts, c.IdentityFile, c.SSHPath, c.TelnetPath} {
		if strings.ContainsAny(s, "\x00\r\n") {
			return c, ErrInvalid
		}
	}
	if c.Protocol == "" {
		c.Protocol = "ssh"
	}
	if c.Protocol != "ssh" && c.Protocol != "telnet" {
		return c, ErrInvalid
	}
	if c.LineEnding == "" {
		c.LineEnding = "\r"
	}
	if c.LineEnding != "\r" && c.LineEnding != "\n" && c.LineEnding != "\r\n" {
		return c, ErrInvalid
	}
	if c.LoginTimeout < 0 || c.CommandTimeout < 0 || c.WriteTimeout < 0 || c.MaxOutputBytes < 0 || c.MaxScriptOutputBytes < 0 {
		return c, ErrInvalid
	}
	if c.LoginTimeout == 0 {
		c.LoginTimeout = 30 * time.Second
	}
	if c.CommandTimeout == 0 {
		c.CommandTimeout = 30 * time.Second
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 10 * time.Second
	}
	if c.MaxOutputBytes == 0 {
		c.MaxOutputBytes = 8 << 20
	}
	if c.MaxScriptOutputBytes == 0 {
		c.MaxScriptOutputBytes = 32 << 20
	}
	return c, nil
}

func pattern(expr string) (expect.Pattern, error) {
	r, err := regexp.Compile(expr)
	if err != nil || r.MatchString("") {
		return expect.Pattern{}, ErrInvalid
	}
	return expect.Compile(expr)
}
func validateInteractions(rules []Interaction) error {
	for _, r := range rules {
		if _, err := pattern(r.Pattern); err != nil || r.Limit < 0 {
			return ErrInvalid
		}
	}
	return nil
}
func snapshot(c Command) (Command, error) {
	if strings.TrimSpace(c.Text) == "" || strings.ContainsAny(c.Text, "\r\n\x00") || c.Timeout < 0 {
		return c, ErrInvalid
	}
	if c.Prompt != "" {
		if _, err := pattern(c.Prompt); err != nil {
			return c, err
		}
	}
	if err := validateInteractions(c.Interactions); err != nil {
		return c, err
	}
	c.Interactions = append([]Interaction(nil), c.Interactions...)
	return c, nil
}

// ParseScript accepts LF/CRLF, skips blank and full-line # comments, and validates
// every line before returning. Inline # and whitespace in commands are preserved.
func ParseScript(text string) ([]Command, error) {
	var commands []Command
	for i, line := range strings.Split(text, "\n") {
		line = strings.TrimSuffix(line, "\r")
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimLeft(line, " \t"), "#") {
			continue
		}
		c, err := snapshot(Command{Text: line, Line: i + 1})
		if err != nil {
			return nil, err
		}
		commands = append(commands, c)
	}
	return commands, nil
}
