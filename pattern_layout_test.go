// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"strconv"
	"testing"
	"unsafe"
)

// Pattern is passed by value through every scan; keep its small fields packed.
func TestPatternLayout(t *testing.T) {
	if strconv.IntSize != 64 {
		t.Skip("layout is asserted on 64-bit platforms")
	}
	if size := unsafe.Sizeof(Pattern{}); size != 80 {
		t.Fatalf("Pattern size = %d, expected 80", size)
	}
}
