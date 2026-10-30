from pathlib import Path
import os
import subprocess
import time

root = Path(__file__).resolve().parent
sources = {
    'expect-before': root/'expect-baseline',
    'expect-after': Path('/Users/crate/Works/go/.worktrees/connector-unification/go-expect'),
    'connector-before': root/'connector-baseline',
    'connector-after': Path('/Users/crate/Works/go/.worktrees/connector-quality/go-net-connector'),
}
env = dict(os.environ, GOWORK='off', GOFLAGS='-mod=readonly', GOTOOLCHAIN='go1.27.3', GOMAXPROCS='4')
groups = [
    ('expect', '.', 'filter', '^BenchmarkSessionFilter$', '100ms'),
    ('expect', '.', 'retention', '^BenchmarkSessionFilterRetention$', '1x'),
    ('expect', '.', 'capture', '^BenchmarkCompetingCaptures$', '100ms'),
    ('expect', '.', 'expect-control', '^Benchmark(ReceivePatterns|FragmentedReceive|PublicRedactor)$', '100ms'),
    ('connector', './netdisco', 'inventory', '^Benchmark(HTTPInventoryDecode|HTTPInventoryLocal|PostgresMapping)$', '100ms'),
    ('connector', './fleet', 'fleet', '^Benchmark(MockFleet|NewPlan)$', '100ms'),
    ('connector', './transfer', 'transfer', '^BenchmarkCompleted$', '100ms'),
    ('connector', './connector', 'connector-control', '^BenchmarkCommand(Output|Paged1000|Multiline|Logging)$', '100ms'),
]
if os.environ.get('GROUPS'):
    groups = [g for g in groups if g[2] in os.environ['GROUPS'].split(',')]
with (root/'benchmark-build.log').open('a') as log:
    for module, package in dict.fromkeys((g[0], g[1]) for g in groups):
        for side in ['before', 'after']:
            binary = root/f'{module}-{package.split("/")[-1]}-{side}.test'
            subprocess.run(['go', 'test', '-c', '-o', str(binary), package], cwd=sources[f'{module}-{side}'], env=env, stdout=log, stderr=log, check=True)

for module, package, group, pattern, duration in groups:
    for side in ['before', 'after']:
        (root/f'{group}-{side}.txt').write_text('')
    for sample in range(10):
        for side in (['before', 'after'] if sample%2 == 0 else ['after', 'before']):
            print(time.strftime('%H:%M:%S'), group, sample+1, side, flush=True)
            binary = root/f'{module}-{package.split("/")[-1]}-{side}.test'
            with (root/f'{group}-{side}.txt').open('a') as log:
                subprocess.run([str(binary), '-test.run=^$', '-test.bench='+pattern, '-test.benchmem', '-test.benchtime='+duration, '-test.timeout=10m'], cwd=sources[f'{module}-{side}'], env=env, stdout=log, stderr=log, check=True)
    with (root/f'{group}-benchstat.txt').open('w') as log:
        subprocess.run(['/tmp/connector-quality-20261029/benchstat', str(root/f'{group}-before.txt'), str(root/f'{group}-after.txt')], stdout=log, stderr=log, check=True)
print('DONE', flush=True)
