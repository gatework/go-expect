//go:build !linux && !darwin

package connector

type backupTarget struct{}

func openBackup(string) (*backupTarget, error) { return nil, ErrUnsupported }
func (*backupTarget) close() error             { return nil }
func (*backupTarget) sync() error              { return ErrUnsupported }
func (*backupTarget) write(string, func() error) (BackupReceipt, error) {
	return BackupReceipt{}, ErrUnsupported
}
