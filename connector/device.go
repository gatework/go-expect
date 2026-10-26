package connector

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	expect "github.com/gatework/go-expect"
)

// Device owns one connection. After Close or an in-flight failure, construct a
// new Device to reconnect explicitly. It never replays a command.
type Device struct {
	cfg           Config
	profile       Profile
	opts          options
	busy          atomic.Bool
	mu            sync.Mutex
	session       *expect.Session
	cancel        context.CancelFunc
	closed, ready bool
	prompt        string
	closeErr      error
	closeDone     chan struct{}
}

func New(profile Profile, cfg Config, opts ...Option) (*Device, error) {
	p, err := profile.clone()
	if err != nil {
		return nil, err
	}
	cfg, err = normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	o := options{dial: openTransport}
	for _, opt := range opts {
		if opt == nil {
			return nil, ErrInvalid
		}
		if err = opt(&o); err != nil {
			return nil, err
		}
	}
	return &Device{profile: p, cfg: cfg, opts: o, closeDone: make(chan struct{})}, nil
}
func (d *Device) String() string        { return "connector.Device" }
func (d *Device) GoString() string      { return d.String() }
func (d *Device) CurrentPrompt() string { d.mu.Lock(); defer d.mu.Unlock(); return d.prompt }
func (d *Device) Connected() bool       { d.mu.Lock(); defer d.mu.Unlock(); return d.ready && !d.closed }
func (d *Device) acquire(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !d.busy.CompareAndSwap(false, true) {
		return ErrBusy
	}
	return nil
}
func (d *Device) release() { d.busy.Store(false) }

// Connect opens, authenticates and learns a complete prompt within LoginTimeout.
// Close can interrupt dialing; custom dialers must honor cancellation.
func (d *Device) Connect(ctx context.Context) (err error) {
	if err = d.acquire(ctx); err != nil {
		return err
	}
	defer d.release()
	ctx, cancel := context.WithTimeout(ctx, d.cfg.LoginTimeout)
	defer cancel()
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrClosed
	}
	if d.ready {
		d.mu.Unlock()
		return nil
	}
	d.cancel = cancel
	d.mu.Unlock()
	returned := false
	defer func() {
		if !returned {
			_ = d.Close()
		}
		if err != nil {
			err = failure("login", errors.Join(err, d.Close()))
		}
	}()
	err = d.openAndLogin(ctx)
	returned = true
	return err
}

func (d *Device) openAndLogin(ctx context.Context) error {
	dialConfig := d.cfg
	dialConfig.Challenges = append([]Interaction(nil), d.cfg.Challenges...)
	stream, err := d.opts.dial(ctx, dialConfig)
	if err != nil {
		if !nilStream(stream) {
			err = errors.Join(err, stream.Close())
		}
		return err
	}
	if nilStream(stream) {
		return ErrInvalid
	}
	s, err := expect.NewSession(stream, expect.WithDefaultTimeout(0), expect.WithWriteTimeout(d.cfg.WriteTimeout), expect.WithBufferLimit(d.cfg.MaxOutputBytes))
	if err != nil {
		return errors.Join(err, stream.Close())
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return errors.Join(ErrClosed, s.Close())
	}
	d.session = s
	d.mu.Unlock()
	for _, secret := range []string{d.cfg.Username, d.cfg.Password, d.cfg.EnablePassword, d.cfg.KeyPassphrase} {
		if secret != "" {
			if err = s.Redact(secret); err != nil {
				return err
			}
		}
	}
	rules := append(append([]Interaction(nil), d.cfg.Challenges...), d.credentials(d.cfg.Password)...)
	if d.cfg.KeyPassphrase != "" {
		rules = append(rules, Interaction{Pattern: `(?i)Enter passphrase for key[^\r\n]*:[ \t]*\z`, Reply: d.cfg.KeyPassphrase + d.cfg.LineEnding, Sensitive: true})
	}
	r, err := d.read(ctx, s, Command{}, d.profile.LoginPrompt, rules, true, d.cfg.MaxOutputBytes)
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	d.ready = true
	d.prompt = r.Prompt
	return nil
}
func nilStream(v io.ReadWriteCloser) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return r.IsNil()
	}
	return false
}
func (d *Device) credentials(password string) []Interaction {
	credential := func(value string) func(context.Context, expect.Result) (string, error) {
		return func(context.Context, expect.Result) (string, error) {
			if value == "" {
				return "", ErrAuthentication
			}
			return value + d.cfg.LineEnding, nil
		}
	}
	return []Interaction{
		{Pattern: d.profile.UsernamePrompt, Respond: credential(d.cfg.Username), Sensitive: true},
		{Pattern: d.profile.PasswordPrompt, Respond: credential(password), Sensitive: true},
	}
}

// Close permanently retires this Device and interrupts an active operation.
// Concurrent Close calls join the same cleanup. No device command is sent.
func (d *Device) Close() error {
	d.mu.Lock()
	if d.closed {
		done := d.closeDone
		d.mu.Unlock()
		<-done
		d.mu.Lock()
		err := d.closeErr
		d.mu.Unlock()
		return err
	}
	d.closed = true
	d.ready = false
	s := d.session
	cancel := d.cancel
	d.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	var err error
	if s != nil {
		err = s.Close()
	}
	d.mu.Lock()
	d.closeErr = err
	close(d.closeDone)
	d.mu.Unlock()
	return err
}

func (d *Device) connectedSession() (*expect.Session, string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, "", ErrClosed
	}
	if !d.ready {
		return nil, "", ErrNotConnected
	}
	return d.session, d.prompt, nil
}

func (d *Device) Execute(ctx context.Context, command Command) (Receipt, error) {
	result, err := d.ExecuteScript(ctx, []Command{command})
	if len(result.Steps) == 0 {
		return Receipt{}, err
	}
	return result.Steps[0], err
}

// ExecuteScript preflights all commands before sending any byte and holds one
// operation lease throughout. The output budget counts raw response bytes.
func (d *Device) ExecuteScript(ctx context.Context, commands []Command) (Result, error) {
	snapshotCommands := make([]Command, len(commands))
	for i, c := range commands {
		var err error
		snapshotCommands[i], err = snapshot(c)
		if err != nil {
			return Result{}, err
		}
	}
	if err := d.acquire(ctx); err != nil {
		return Result{}, err
	}
	defer d.release()
	if _, _, err := d.connectedSession(); err != nil {
		return Result{}, err
	}
	var result Result
	remaining := d.cfg.MaxScriptOutputBytes
	for _, c := range snapshotCommands {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if remaining <= 0 {
			return result, ErrOutputLimit
		}
		r, err := d.execute(ctx, c, min(remaining, d.cfg.MaxOutputBytes), nil, nil)
		if r.Command != "" {
			result.Steps = append(result.Steps, r)
		}
		remaining -= len(r.Raw)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

// Enable performs the profile's explicit privilege command. A password is sent
// only in response to a matching credential prompt, at most once.
func (d *Device) Enable(ctx context.Context) (Receipt, error) {
	if d.profile.Enable.Text == "" {
		return Receipt{}, ErrUnsupported
	}
	if err := d.acquire(ctx); err != nil {
		return Receipt{}, err
	}
	defer d.release()
	password := d.cfg.EnablePassword
	if password == "" {
		password = d.cfg.Password
	}
	return d.execute(ctx, d.profile.Enable, d.cfg.MaxOutputBytes, d.credentials(password), nil)
}

func (d *Device) execute(parent context.Context, c Command, budget int, extra []Interaction, check func(Receipt) error) (r Receipt, err error) {
	s, prompt, err := d.connectedSession()
	if err != nil {
		return r, err
	}
	timeout := c.Timeout
	if timeout == 0 {
		timeout = d.cfg.CommandTimeout
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	if err = ctx.Err(); err != nil {
		return r, err
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return r, ErrClosed
	}
	d.cancel = cancel
	d.mu.Unlock()
	started := time.Now()
	r.Command = c.Text
	finished, returned := false, false
	defer func() {
		if !finished {
			_ = d.Close()
		}
	}()
	// Once send is attempted, every error retires ambiguous transport state.
	defer func() {
		if !returned {
			return
		}
		r.Duration = time.Since(started)
		r.Err = failure("command", err)
		if d.opts.receipt != nil {
			if callbackErr := d.opts.receipt(r); callbackErr != nil {
				err = errors.Join(err, callbackErr)
			}
		}
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			err = errors.Join(err, d.Close())
		}
		r.Err = failure("command", err)
		err = r.Err
		finished = true
	}()
	if c.Sensitive {
		if err = s.Redact(c.Text); err != nil {
			returned = true
			return r, err
		}
	}
	r.Written, err = s.Send(ctx, c.Text+d.cfg.LineEnding)
	if err != nil {
		returned = true
		return r, err
	}
	completion := c.Prompt
	if completion == "" {
		completion = exactPrompt(prompt)
	}
	rules := append(append(append([]Interaction(nil), c.Interactions...), extra...), d.profile.Paging...)
	response, readErr := d.read(ctx, s, c, completion, rules, false, budget)
	response.Written = r.Written
	response.Command = c.Text
	r = response
	err = readErr
	if r.Complete {
		d.mu.Lock()
		d.prompt = r.Prompt
		d.mu.Unlock()
	}
	if err == nil && check != nil {
		err = check(r)
	}
	if err == nil {
		err = ctx.Err()
	}
	returned = true
	return r, err
}

func cleanOutput(output, command string) string {
	output = strings.ReplaceAll(output, "\r\n", "\n")
	if command != "" {
		output = strings.TrimPrefix(output, command+"\n")
	}
	return output
}
