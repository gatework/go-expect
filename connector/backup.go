package connector

import (
	"context"
	"errors"
	"time"
)

// BackupReceipt distinguishes atomic replacement from directory durability.
// Even with a returned error, Committed may be true: never replay on that basis.
type BackupReceipt struct {
	Path               string
	Bytes              int
	SHA256             string
	CollectedAt        time.Time
	Committed, Durable bool
}

func (BackupReceipt) String() string     { return "connector.BackupReceipt" }
func (r BackupReceipt) GoString() string { return r.String() }

// Backup holds a nonblocking destination lock through collection and persistence.
// The caller must provide a trusted existing directory. It never creates one.
func (d *Device) Backup(ctx context.Context, path string) (receipt BackupReceipt, err error) {
	if ctx == nil {
		return receipt, ErrInvalid
	}
	if err = ctx.Err(); err != nil {
		return receipt, err
	}
	target, err := openBackup(path)
	if err != nil {
		return receipt, failure("backup", err)
	}
	defer func() { err = failure("backup", errors.Join(err, target.close())) }()
	if err = d.acquire(ctx); err != nil {
		return receipt, err
	}
	defer d.release()
	configuration, err := d.runningConfig(ctx)
	if err != nil {
		return receipt, err
	}
	if err = ctx.Err(); err != nil {
		return receipt, err
	}
	receipt, err = target.write(configuration.Text, target.sync)
	receipt.CollectedAt = time.Now()
	return receipt, err
}
