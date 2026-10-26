//go:build !linux && !darwin

package connector

import (
	"context"
	"io"
)

func openTransport(context.Context, Config) (io.ReadWriteCloser, error) { return nil, ErrUnsupported }
