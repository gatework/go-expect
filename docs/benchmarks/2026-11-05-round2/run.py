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
import("context";"fmt";"runtime";"strings";"testing")
func BenchmarkAuditLineSetup(b *testing.B){p:=Line(strings.Repeat("x",4096));b.ReportAllocs();for b.Loop(){c:=newMatchCursor(p);runtime.KeepAlive(c)}}
func BenchmarkAuditResponder(b *testing.B){
 for _,count:=range []int{0,1,8,64}{b.Run(fmt.Sprint(count),func(b *testing.B){
 rules:=make([]Rule,count);for i:=range rules{rules[i]=Rule{Pattern:Literal(fmt.Sprint("rule_",i)),Respond:func(context.Context,*Session,Result)error{return nil}}}
 r,err:=NewResponder(rules...);if err!=nil{b.Fatal(err)}
 s,err:=NewSession(&admissionTransport{closed:make(chan struct{})},WithDefaultTimeout(0));if err!=nil{b.Fatal(err)};defer s.Close()
 data:=[]byte("!");completion:=[]Pattern{Literal("!")};b.ReportAllocs()
 for b.Loop(){s.pending=data;if _,err:=r.Expect(b.Context(),s,completion);err!=nil{b.Fatal(err)}}
 })}
}
func BenchmarkAuditNoOutput(b *testing.B){secret:=strings.Repeat("s",4096);b.ReportAllocs();for b.Loop(){
 s,err:=NewSession(&admissionTransport{closed:make(chan struct{})});if err!=nil{b.Fatal(err)}
 if err=s.Redact(secret);err!=nil{b.Fatal(err)};if err=s.Close();err!=nil{b.Fatal(err)}
}}
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
        source = common
        backing = tmp / (label + "_test.go")
        backing.write_text(source)
        overlay = tmp / (label + ".json")
        overlay.write_text(json.dumps({"Replace": {str(root / "zz_audit_bench_test.go"): str(backing)}}))
        mapping=json.loads(overlay.read_text())
        for shared in ["quality_bench_test.go","refinement_test.go"]:
            mapping["Replace"][str(root/shared)]=str(repo/shared)
        overlay.write_text(json.dumps(mapping))
        overlays[label] = (root, overlay)
    env = dict(os.environ, GOMAXPROCS="4", GOWORK="off")
    for label in overlays:
        (out / (label + ".txt")).write_text("")
    for sample in range(10):
        for label in (("before", "after") if sample % 2 == 0 else ("after", "before")):
            root, overlay = overlays[label]
            result = subprocess.run(["go", "test", "-overlay", str(overlay), "-run", "^$", "-bench", "^(BenchmarkAudit.*|BenchmarkReceivePatterns|BenchmarkPublicRedactor|BenchmarkPTYReceive|BenchmarkNestedValidation|BenchmarkBufferedExpect)$", "-benchmem", "-benchtime=100ms", "-count=1", "."], cwd=root, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
            with (out / (label + ".txt")).open("a") as output:
                output.write("\n".join(line.rstrip() for line in result.stdout.splitlines()) + "\n")
        print("sample", sample + 1, flush=True)
    hashes = {str(p.relative_to(repo)): hashlib.sha256(p.read_bytes()).hexdigest() for p in repo.glob("*.go")}
    hashes["baseline-zip"] = hashlib.sha256(archive.read_bytes()).hexdigest()
    (out / "sha256.json").write_text(json.dumps(hashes, indent=2) + "\n")
