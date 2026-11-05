"""Run serial interleaved samples against the supplied pre-change source ZIP."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import zipfile

repo = Path.cwd()
out = Path(__file__).resolve().parent
archive = Path(sys.argv[1]).resolve()
common = r'''package expect
import("bytes";"context";"net";"runtime";"testing")
func BenchmarkAuditLine(b *testing.B){
 data:=bytes.Repeat([]byte("x"),64<<10);p:=Line("sw#")
 INIT
 b.ReportAllocs()
 for b.Loop(){RESET;for end:=32;end<=len(data);end+=32{FIND}}
}
func BenchmarkAuditNested(b *testing.B){p:=Literal("x");for range 1024{p=All(p)};b.ReportAllocs();for b.Loop(){if err:=p.ValidateText();err!=nil{b.Fatal(err)}}}
func BenchmarkAuditSnapshots(b *testing.B){
 data:=bytes.Repeat([]byte("x"),64<<10);offsets:=make([]int,128);for i:=0;i<len(offsets);i+=2{offsets[i+1]=len(data)}
 loc:=location{start:0,end:len(data),submatches:offsets};match:=string(data);runtime.KeepAlive(match);b.ReportAllocs()
 for b.Loop(){captures:=CAPTURE;runtime.KeepAlive(captures)}
}
func BenchmarkAuditBuffered(b *testing.B){
 a,peer:=net.Pipe();defer peer.Close();s,err:=NewSession(a,WithDefaultTimeout(0));if err!=nil{b.Fatal(err)};defer s.Close()
 data:=[]byte("!");cases:=[]Case{{Pattern:Literal("!")}};b.ReportAllocs()
 for b.Loop(){s.pending=data;if _,err:=s.Expect(context.Background(),cases);err!=nil{b.Fatal(err)}}
}
'''
with tempfile.TemporaryDirectory(prefix="expect-comparison-") as tmp:
    tmp = Path(tmp)
    baseline = tmp / "baseline"
    baseline.mkdir()
    with zipfile.ZipFile(archive) as z:
        for name in z.namelist():
            if not name.startswith("go-expect/") or name.endswith("/"):
                continue
            relative = Path(name.removeprefix("go-expect/"))
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("unsafe archive member")
            if relative.suffix != ".go" and str(relative) not in ("go.mod", "go.sum"):
                continue
            dest = baseline / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(z.read(name))
    overlays = {}
    for label, root in (("before", baseline), ("after", repo)):
        source = common.replace("INIT", "c:=newMatchCursor(p)" if label == "after" else "")
        source = source.replace("RESET", "c.reset()" if label == "after" else "")
        source = source.replace("FIND", "c.find(data[:end],false,0,true)" if label == "after" else "p.find(data[:end],false,0)")
        source = source.replace("CAPTURE", "loc.capturesFromSnapshot(data,match)" if label == "after" else "loc.materializeCaptures(data)")
        backing = tmp / (label + "_test.go")
        backing.write_text(source)
        overlay = tmp / (label + ".json")
        overlay.write_text(json.dumps({"Replace": {str(root / "zz_audit_bench_test.go"): str(backing)}}))
        overlays[label] = (root, overlay)
    env = dict(os.environ, GOMAXPROCS="4", GOWORK="off")
    for label in overlays:
        (out / (label + ".txt")).write_text("")
    for sample in range(10):
        for label in (("before", "after") if sample % 2 == 0 else ("after", "before")):
            root, overlay = overlays[label]
            result = subprocess.run(["go", "test", "-overlay", str(overlay), "-run", "^$", "-bench", "^(BenchmarkAudit.*|BenchmarkReceivePatterns|BenchmarkPublicRedactor)$", "-benchmem", "-benchtime=100ms", "-count=1", "."], cwd=root, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
            with (out / (label + ".txt")).open("a") as output:
                output.write("\n".join(line.rstrip() for line in result.stdout.splitlines()) + "\n")
        print("sample", sample + 1, flush=True)
    hashes = {str(p.relative_to(repo)): hashlib.sha256(p.read_bytes()).hexdigest() for p in repo.glob("*.go")}
    hashes["baseline-zip"] = hashlib.sha256(archive.read_bytes()).hexdigest()
    (out / "sha256.json").write_text(json.dumps(hashes, indent=2) + "\n")
