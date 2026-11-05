// SPDX-License-Identifier: Apache-2.0

package expect

// WithRemoveNulls explicitly removes NUL bytes from the matching/receive view.
// The default preserves every transport byte.
func WithRemoveNulls(enabled bool) SessionOption {
	return func(o *sessionOptions) error { o.removeNulls = enabled; return nil }
}

// WithStripParity explicitly clears the high bit before optional NUL removal.
func WithStripParity(enabled bool) SessionOption {
	return func(o *sessionOptions) error { o.stripParity = enabled; return nil }
}
