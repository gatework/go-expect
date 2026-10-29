from pathlib import Path
import os, subprocess, time
root=Path('/tmp/connector-refinement-20261029')
env=os.environ.copy()
env['GOMAXPROCS']='4'
env['GOWORK']='off'
started=time.time()
patterns=[('redactor-final', r'^Benchmark(PublicRedactor|PublicRedactorLongBoundary|PublicRedactorDenseLong|PublicRedactorMixedDenseByte|RedactorRetainedTail)$', '100ms')]
patterns += [(f'idle-heap-final-{count}', f'^BenchmarkOSIdleHeap$/{count}$', '3x') for count in (1,64,256,1024)]
for kind, pattern, benchtime in patterns:
 for sample in range(10):
  order=('baseline','candidate') if sample%2==0 else ('candidate','baseline')
  for variant in order:
   args=[str(root/f'expect-{variant}.test'),'-test.run=^$',f'-test.bench={pattern}',f'-test.benchtime={benchtime}','-test.benchmem','-test.count=1','-test.timeout=5m']
   run=subprocess.run(args,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
   with (root/f'expect-{kind}-{variant}.txt').open('a') as f: f.write(run.stdout)
   print(f'{kind} sample={sample+1}/10 variant={variant} exit={run.returncode} elapsed={time.time()-started:.1f}s',flush=True)
   if run.returncode: raise SystemExit(run.returncode)
print(f'COMPLETE elapsed={time.time()-started:.1f}s',flush=True)
