//go:build linux || darwin

package connector

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

type backupTarget struct {
	root       *os.Root
	lock       *os.File
	name, path string
}

func openBackup(path string) (target *backupTarget, err error) {
	if path == "" {
		return nil, ErrInvalid
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	target = &backupTarget{root: root, name: filepath.Base(path), path: path}
	allocated := target
	success := false
	defer func() {
		if !success {
			err = errors.Join(err, allocated.close())
		}
	}()
	if err = target.check(); err != nil {
		return nil, err
	}
	// Stable sidecars are never removed. Hashing keeps filenames bounded and
	// separates lock names from device-controlled configuration content.
	canonicalName := norm.NFC.String(cases.Fold().String(norm.NFC.String(target.name)))
	lockName := fmt.Sprintf(".go-expect-%x.lock", sha256.Sum256([]byte(canonicalName)))
	target.lock, err = root.OpenFile(lockName, os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	info, err := target.lock.Stat()
	if err != nil {
		return nil, err
	}
	var st unix.Stat_t
	if err = unix.Fstat(int(target.lock.Fd()), &st); err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || st.Nlink != 1 || st.Uid != uint32(os.Geteuid()) {
		return nil, ErrInvalid
	}
	if err = unix.Flock(int(target.lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrBusy
		}
		return nil, err
	}
	current, err := root.Lstat(lockName)
	if err != nil {
		return nil, err
	}
	if !os.SameFile(info, current) {
		return nil, ErrInvalid
	}
	success = true
	return target, nil
}
func (t *backupTarget) check() error {
	info, err := t.root.Lstat(t.name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return ErrInvalid
	}
	return nil
}
func (t *backupTarget) close() error {
	if t == nil {
		return nil
	}
	var err error
	if t.lock != nil {
		err = t.lock.Close()
	}
	return errors.Join(err, t.root.Close())
}
func (t *backupTarget) sync() error {
	dir, err := t.root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
func (t *backupTarget) write(text string, syncDir func() error) (receipt BackupReceipt, err error) {
	receipt = BackupReceipt{Path: t.path, Bytes: len(text), SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(text)))}
	if err = t.check(); err != nil {
		return receipt, err
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return receipt, err
	}
	name := fmt.Sprintf(".go-expect-temp-%x", nonce)
	file, err := t.root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return receipt, err
	}
	defer func() {
		if !receipt.Committed {
			_ = t.root.Remove(name)
		}
	}()
	n, writeErr := io.WriteString(file, text)
	if writeErr == nil && n != len(text) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	writeErr = errors.Join(writeErr, file.Close())
	if writeErr != nil {
		return receipt, writeErr
	}
	if err = t.root.Rename(name, t.name); err != nil {
		return receipt, err
	}
	receipt.Committed = true
	if err = syncDir(); err != nil {
		return receipt, err
	}
	receipt.Durable = true
	return receipt, nil
}
