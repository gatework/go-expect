// SPDX-License-Identifier: Apache-2.0

package spawn

import (
	"cmp"
	"slices"
	"strings"
)

// windowsEnvironment returns the entries of a Windows environment block. Names
// compare case-insensitively, later duplicates win, and drive-relative entries
// such as "=C:=C:\dir" keep their leading '=' as part of the name. Entries are
// sorted by name, not by the whole "name=value" string, because '=' sorts after
// characters that may appear in names.
func windowsEnvironment(env []string) []string {
	type variable struct{ name, entry string }
	vars := make([]variable, 0, len(env))
	seen := make(map[string]bool, len(env))
	for _, entry := range slices.Backward(env) {

		separator := strings.IndexByte(entry, '=')
		if separator == 0 {
			if next := strings.IndexByte(entry[1:], '='); next >= 0 {
				separator = next + 1
			}
		}
		if separator < 0 {
			separator = len(entry)
		}
		name := strings.ToUpper(entry[:separator])
		if !seen[name] {
			seen[name] = true
			vars = append(vars, variable{name: name, entry: entry})
		}
	}
	slices.SortStableFunc(vars, func(a, b variable) int { return cmp.Compare(a.name, b.name) })
	entries := make([]string, len(vars))
	for i, v := range vars {
		entries[i] = v.entry
	}
	return entries
}
