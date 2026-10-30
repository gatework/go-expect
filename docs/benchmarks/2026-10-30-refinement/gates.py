from pathlib import Path
import os, subprocess, shutil
root=Path(__file__).resolve().parent
expect=Path('/Users/crate/Works/go/.worktrees/connector-unification/go-expect')
connector=Path('/Users/crate/Works/go/.worktrees/connector-quality/go-net-connector')
env=dict(os.environ,GOWORK='off',GOFLAGS='-mod=readonly',GOTOOLCHAIN='local',GOMAXPROCS='4')
env['PATH']='/Users/crate/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.27.3.darwin-arm64/bin:'+env['PATH']
def run(name,cwd,cmd,environment=env):
 print(name,flush=True)
 with (root/(name+'.log')).open('w') as log:
  subprocess.run(cmd,cwd=cwd,env=environment,stdout=log,stderr=log,check=True)
run('format-imports',connector,['go','run','golang.org/x/tools/cmd/goimports@v0.52.0','-w','connector/host_trust.go','connector/transport_round3_test.go','netdisco/inventory_decode_test.go','netdisco/inventory_decode_oracle_test.go','netdisco/inventory_decode_bench_test.go'])
run('expect-ci',expect,['bash','script/ci'])
run('connector-ci',connector,['bash','scripts/ci'])
for name,path in [('expect',expect),('connector',connector)]:
 run(name+'-lint',path,['/tmp/go-expect-ci-tools/golangci-lint','run','./...'])
 run(name+'-vuln',path,['/tmp/go-expect-ci-tools/govulncheck','./...'])
 run(name+'-darwin126',path,['/bin/sh','-c','go version && go test -race -shuffle=on -count=1 ./... && go vet ./...'],dict(env,PATH='/Users/crate/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64/bin:'+env['PATH'],NC_TEST_POSTGRES_BIN='/opt/homebrew/opt/postgresql@18/bin'))
run('expect-release-tests',expect,['python3','-m','unittest','discover','-s','script/tests','-v'])
run('connector-release-tests',connector,['python3','scripts/test_release.py','-v'])
run('paired-source',connector,['python3','scripts/verify-expect-source.py',str(expect)])
run('firemon-source',connector,['python3','scripts/verify-firemon-source.py','/Users/crate/Works/go/firemon/adapter/netconnector'])
for name,path in [('expect',expect),('connector',connector)]:
 target=root/(name+'-linux-source')
 if target.exists(): shutil.rmtree(target)
 shutil.copytree(path,target,ignore=shutil.ignore_patterns('.git','.reference','.superpowers','.work','bin','*.pprof'))
 script='''set -eu
export GOWORK=off GOFLAGS=-mod=readonly GOTOOLCHAIN=local GOMODCACHE=/gomodcache GOMAXPROCS=4
for version in 1.26.0 1.27.3; do
 if [ "$version" = 1.26.0 ]; then go_bin=/gomodcache/golang.org/toolchain@v0.0.1-go1.26.0.linux-arm64/bin/go; else go_bin=/usr/local/go/bin/go; fi
 "$go_bin" version
'''
 if name=='connector': script+=' "$go_bin" build -o /tmp/worker ./cmd/net-connector-archive\n export NC_TEST_ARCHIVE_WORKER=/tmp/worker\n'
 script+=' "$go_bin" test -race -shuffle=on -count=1 -timeout=180s ./...\n "$go_bin" vet ./...\ndone\n'
 (target/'round3-linux.sh').write_text(script)
 run(name+'-linux',path,['docker','run','--rm','--network','none','--mount','type=bind,src='+str(target)+',dst=/source','--mount','type=bind,src=/Users/crate/go/pkg/mod,dst=/gomodcache,readonly','-w','/source','golang:1.27.3','sh','round3-linux.sh'])
print('ALL GATES PASS',flush=True)
