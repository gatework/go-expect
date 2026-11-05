# Performance evidence

This directory retains historical benchstat comparisons and compact summaries.
Raw CI logs, captured source variants and one-off runner scripts were removed
from the main tree. They remain recoverable in Git at v0.1.4. New raw measurements
belong in external build artifacts, with source revision and toolchain metadata.

Reproduce current measurements with `go test -run '^$' -bench . -benchmem .`.
Compare fixed baseline/candidate revisions with repeated runs and benchstat;
three-iteration CI benchmark smokes detect breakage, not statistically significant
performance differences. New capture fields and rule metadata intentionally
change snapshot storage, so compare workloads as well as aggregate bytes.
