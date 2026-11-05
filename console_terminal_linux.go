//go:build linux

// SPDX-License-Identifier: Apache-2.0

package expect

import "golang.org/x/sys/unix"

const termiosRead = unix.TCGETS
const termiosWrite = unix.TCSETS
