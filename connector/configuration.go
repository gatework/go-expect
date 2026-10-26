package connector

import (
	"context"
	"regexp"
	"strings"
)

// Configuration contains explicitly validated, sensitive configuration text.
type Configuration struct {
	Text    string
	Receipt Receipt
}

func (Configuration) String() string               { return "connector.Configuration" }
func (c Configuration) GoString() string           { return c.String() }
func (Configuration) MarshalJSON() ([]byte, error) { return []byte(`{}`), nil }

func (d *Device) RunningConfig(ctx context.Context) (Configuration, error) {
	if d.profile.RunningConfig.Text == "" || d.profile.ValidateConfig == nil {
		return Configuration{}, ErrUnsupported
	}
	if err := d.acquire(ctx); err != nil {
		return Configuration{}, err
	}
	defer d.release()
	return d.runningConfig(ctx)
}
func (d *Device) runningConfig(ctx context.Context) (result Configuration, err error) {
	if d.profile.RunningConfig.Text == "" || d.profile.ValidateConfig == nil {
		return result, ErrUnsupported
	}
	var text string
	result.Receipt, err = d.execute(ctx, d.profile.RunningConfig, d.cfg.MaxOutputBytes, nil, func(r Receipt) error {
		var validationErr error
		text, validationErr = d.profile.ValidateConfig(r.Output)
		if validationErr == nil && strings.TrimSpace(text) == "" {
			validationErr = ErrIncomplete
		}
		return validationErr
	})
	if err == nil {
		result.Text = text
	}
	return result, err
}

// SaveConfig requires the profile's explicit success marker in addition to the
// final prompt. It cannot prove flash-media durability or survive-power-loss state.
func (d *Device) SaveConfig(ctx context.Context) (Receipt, error) {
	if d.profile.SaveConfig.Text == "" || d.profile.SaveSuccess == "" {
		return Receipt{}, ErrUnsupported
	}
	if err := d.acquire(ctx); err != nil {
		return Receipt{}, err
	}
	defer d.release()
	return d.execute(ctx, d.profile.SaveConfig, d.cfg.MaxOutputBytes, nil, func(r Receipt) error {
		if !regexp.MustCompile(d.profile.SaveSuccess).MatchString(r.Output) {
			return ErrUnconfirmed
		}
		return nil
	})
}
