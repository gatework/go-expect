// SPDX-License-Identifier: Apache-2.0

package expect

import "testing"

func BenchmarkInputMailbox(b *testing.B) {
	for _, chunks := range []int{1, 16} {
		name := "single"
		if chunks > 1 {
			name = "coalesced"
		}
		b.Run(name, func(b *testing.B) {
			var mailbox inputMailbox
			data := make([]byte, 64)
			b.ReportAllocs()
			for b.Loop() {
				for range chunks {
					if err := mailbox.enqueue(readEvent{data: data}, 1024); err != nil {
						b.Fatal(err)
					}
				}
				event, ok := mailbox.takeQueued()
				if !ok || len(event.data) != chunks*len(data) {
					b.Fatal("queued input was lost")
				}
			}
		})
	}
}
