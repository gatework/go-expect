//go:build !linux && !darwin && !freebsd && !windows

// SPDX-License-Identifier: Apache-2.0

package spawn

import (
	"context"
	expect "github.com/gatework/go-expect"
)

type processState struct{}

func startProcess(context.Context, string, []string, options) (*processState, error) {
	return nil, ErrUnsupported
}
func (*processState) sessionHandle() *expect.Session { return nil }
func (*processState) pid() int                       { return 0 }
func (*processState) wait() (ExitStatus, error)      { return ExitStatus{}, ErrUnsupported }
func (*processState) close() error                   { return ErrUnsupported }
func (*processState) terminal() Terminal             { return unsupportedTerminal{} }

type unsupportedTerminal struct{}

func (unsupportedTerminal) SetSize(int, int) error { return ErrUnsupported }
