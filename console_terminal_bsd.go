//go:build darwin || freebsd

// SPDX-License-Identifier: Apache-2.0

package expect

import "golang.org/x/sys/unix"

const termiosRead = unix.TIOCGETA
const termiosWrite = unix.TIOCSETA
