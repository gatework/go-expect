// SPDX-License-Identifier: Apache-2.0

package spawn

import (
	"slices"
	"testing"
)

func TestWindowsEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    []string
		expected []string
	}{
		{"sorts by name before separator", []string{"A=y", "A1=x", "A_B=z"}, []string{"A=y", "A1=x", "A_B=z"}},
		{"case insensitive names", []string{"b=2", "A=1", "C=3"}, []string{"A=1", "b=2", "C=3"}},
		{"later duplicate wins", []string{"Path=old", "PATH=new"}, []string{"PATH=new"}},
		{"drive relative entries", []string{"Z=1", "=C:=C:\\dir"}, []string{"=C:=C:\\dir", "Z=1"}},
		{"empty", []string{}, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := windowsEnvironment(tc.input); !slices.Equal(got, tc.expected) {
				t.Fatalf("got %q, expected %q", got, tc.expected)
			}
		})
	}
}
