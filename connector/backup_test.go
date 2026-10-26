//go:build linux || darwin

package connector

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBackupValidatedPrivateReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "router.cfg")
	if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	d := deviceFixture(t, CiscoIOS(), Config{}, "r1#", []exchange{{"show running-config\r", "show running-config\r\nhostname r1\r\nend\r\nr1#"}})
	r, err := d.Backup(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "hostname r1\nend\n" || !r.Committed || !r.Durable || r.Bytes != len(content) || r.SHA256 != fmt.Sprintf("%x", sha256.Sum256(content)) {
		t.Fatalf("backup evidence: %+v %q", r, content)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("private backup: %v", err)
	}
}

func TestIncompleteBackupPreservesPrevious(t *testing.T) {
	path := filepath.Join(t.TempDir(), "router.cfg")
	if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	d := deviceFixture(t, CiscoIOS(), Config{}, "r1#", []exchange{{"show running-config\r", "show running-config\r\nr1#"}})
	r, err := d.Backup(t.Context(), path)
	if !errors.Is(err, ErrIncomplete) || r.Committed {
		t.Fatalf("empty collection accepted: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "old" {
		t.Fatal("previous backup changed")
	}
}

func TestSaveRequiresSuccessEvidence(t *testing.T) {
	var observed Receipt
	d := deviceFixture(t, CiscoIOS(), Config{}, "r1#", []exchange{{"copy running-config startup-config\r", "Destination filename [startup-config]?"}, {"\r", "\r\nr1#"}}, WithReceipt(func(r Receipt) error { observed = r; return nil }))
	r, err := d.SaveConfig(t.Context())
	if !errors.Is(err, ErrUnconfirmed) || !r.Complete {
		t.Fatalf("save evidence: %v", err)
	}
	if !errors.Is(observed.Err, ErrUnconfirmed) || !observed.Complete {
		t.Fatal("observer received premature success")
	}
}

func TestSaveDoesNotTreatNegativeSentenceAsSuccess(t *testing.T) {
	d := deviceFixture(t, Huawei(), Config{}, "<r1>", []exchange{{"save force\r", "save force\r\nConfiguration was not saved successfully.\r\n<r1>"}})
	if _, err := d.SaveConfig(t.Context()); !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("negative save report accepted: %v", err)
	}
}

func TestSaveAcceptsExplicitSuccess(t *testing.T) {
	for _, p := range []Profile{CiscoIOS(), Huawei(), H3C()} {
		t.Run(p.Name, func(t *testing.T) {
			prompt, marker := "r1#", "[OK]"
			if p.Name != "cisco_ios" {
				prompt = "<r1>"
				marker = "Info: Save the configuration successfully."
			}
			d := deviceFixture(t, p, Config{}, prompt, []exchange{{p.SaveConfig.Text + "\r", p.SaveConfig.Text + "\r\n" + marker + "\r\n" + prompt}})
			if r, err := d.SaveConfig(t.Context()); err != nil || !r.Complete {
				t.Fatalf("save success: %v", err)
			}
		})
	}
}

func TestBackupLocksFilenameAliases(t *testing.T) {
	for _, names := range [][2]string{{"router.cfg", "ROUTER.cfg"}, {"caf\u00e9.cfg", "cafe\u0301.cfg"}} {
		t.Run(names[0], func(t *testing.T) {
			dir := t.TempDir()
			target, err := openBackup(filepath.Join(dir, names[0]))
			if err != nil {
				t.Fatal(err)
			}
			defer target.close()
			other, err := openBackup(filepath.Join(dir, names[1]))
			if err == nil {
				other.close()
				t.Fatal("alias acquired a second lock")
			}
			if !errors.Is(err, ErrBusy) {
				t.Fatal(err)
			}
		})
	}
}

func TestBackupRejectsTerminalControlBytes(t *testing.T) {
	d := deviceFixture(t, CiscoIOS(), Config{}, "r1#", []exchange{{"show running-config\r", "show running-config\r\nhostname r1\x1b[2J\r\nend\r\nr1#"}})
	_, err := d.Backup(t.Context(), filepath.Join(t.TempDir(), "router.cfg"))
	if !errors.Is(err, ErrIncomplete) {
		t.Fatalf("terminal control accepted in config: %v", err)
	}
}

func TestBackupLockAndSymlinkRejection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "router.cfg")
	target, err := openBackup(path)
	if err != nil {
		t.Fatal(err)
	}
	defer target.close()
	if _, err = openBackup(path); !errors.Is(err, ErrBusy) {
		t.Fatalf("same path lock: %v", err)
	}
	other := filepath.Join(dir, "other.cfg")
	if err = os.Symlink("router.cfg", other); err != nil {
		t.Fatal(err)
	}
	if _, err = openBackup(other); !errors.Is(err, ErrInvalid) {
		t.Fatalf("symlink accepted: %v", err)
	}
}

func TestPostRenameFailureCarriesCommittedReceipt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "router.cfg")
	target, err := openBackup(path)
	if err != nil {
		t.Fatal(err)
	}
	defer target.close()
	marker := errors.New("directory sync failure")
	receipt, err := target.write("new configuration", func() error { return marker })
	if !errors.Is(err, marker) || !receipt.Committed || receipt.Durable {
		t.Fatalf("lost commit evidence: %v %+v", err, receipt)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "new configuration" {
		t.Fatal("rename did not commit")
	}
	files, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if strings.HasPrefix(f.Name(), ".go-expect-temp-") {
			t.Fatal("temporary file leaked")
		}
	}
}

func TestRejectedBackupReleasesDirectoryDescriptors(t *testing.T) {
	count := func() int {
		f, err := os.Open("/dev/fd")
		if err != nil {
			t.Fatal(err)
		}
		names, err := f.Readdirnames(-1)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		return len(names)
	}
	path := filepath.Join(t.TempDir(), "backup")
	target, err := openBackup(path)
	if err != nil {
		t.Fatal(err)
	}
	defer target.close()
	before := count()
	for range 20 {
		if _, err := openBackup(path); !errors.Is(err, ErrBusy) {
			t.Fatal(err)
		}
	}
	if after := count(); after > before {
		t.Fatalf("rejected backups leaked %d descriptors", after-before)
	}
}
