from pathlib import Path
import os, subprocess, time
root=Path('/tmp/connector-refinement-20261029')
env=os.environ.copy()
env['GOMAXPROCS']='4'
env['GOWORK']='off'
patterns={
 'throughput': (r'^Benchmark(FragmentedReceive|ReceivePatterns|PublicRedactor|PublicRedactorLongBoundary|RedactorRetainedTail|OSReceive|PTYReceive)$', '100ms'),
 'dense': (r'^BenchmarkPublicRedactor(DenseLong|MixedDenseByte)$', '100ms'),
 'retention': (r'^BenchmarkReceiveRemainderRetention$', '1x'),
}
started=time.time()
for kind,(pattern,benchtime) in patterns.items():
 for sample in range(10):
  order=('baseline','candidate') if sample%2==0 else ('candidate','baseline')
  for variant in order:
   args=[str(root/f'expect-{variant}.test'),'-test.run=^$',f'-test.bench={pattern}',f'-test.benchtime={benchtime}','-test.benchmem','-test.count=1','-test.timeout=5m']
   run=subprocess.run(args,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
   with (root/f'expect-{kind}-{variant}.txt').open('a') as f:
    f.write(run.stdout)
   print(f'{kind} sample={sample+1}/10 variant={variant} exit={run.returncode} elapsed={time.time()-started:.1f}s',flush=True)
   if run.returncode: raise SystemExit(run.returncode)
for count in (1,64,256,1024):
 for sample in range(10):
  order=('baseline','candidate') if sample%2==0 else ('candidate','baseline')
  for variant in order:
   args=[str(root/f'expect-{variant}.test'),'-test.run=^$',f'-test.bench=^BenchmarkOSIdlePopulation$/{count}$','-test.benchtime=3x','-test.benchmem','-test.count=1','-test.timeout=5m']
   run=subprocess.run(args,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
   with (root/f'expect-idle-{variant}.txt').open('a') as f:
    f.write(run.stdout)
   print(f'idle count={count} sample={sample+1}/10 variant={variant} exit={run.returncode} elapsed={time.time()-started:.1f}s',flush=True)
   if run.returncode: raise SystemExit(run.returncode)
print(f'COMPLETE elapsed={time.time()-started:.1f}s',flush=True)
