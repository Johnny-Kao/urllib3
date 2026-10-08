#!/usr/bin/env python3
"""PR5304 pinned, same-runner HTTP / decoder A/B; local fixture, not production traffic."""
import argparse, concurrent.futures, gc, gzip, hashlib, json, os, pathlib, random
import statistics, subprocess, sys, threading, time, tracemalloc
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import psutil

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "ab-results-v2"
BASE = "a164d79c8cf760f222daa2dc7f67d0e1ca7fb17c"
NEW = "6ae668b355863f72d4381c4696c4748cd386a7d6"

def data_for(kind, mib):
    n = mib * 1048576
    if kind == "json":
        return (b'{"id":42,"name":"payload","status":"ok"}\n' * ((n + 40)//41))[:n]
    if kind == "compressible":
        return (b"0123456789ABCDEF" * ((n+15)//16))[:n]
    return random.Random(9472+mib).randbytes(n)

def serve(args):
    raw = data_for(args.kind, args.mib)
    zipped = gzip.compress(raw, compresslevel=6, mtime=0) if args.kind != "plain" else raw
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(zipped)))
            if args.kind != "plain":
                self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Type", "application/json" if args.kind == "json" else "application/octet-stream")
            self.end_headers()
            # Serve identical compressed bytes with the same write pattern.
            for offset in range(0,len(zipped),65536):
                self.wfile.write(zipped[offset:offset+65536])
        def log_message(self, *_): pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    print(json.dumps({"port":server.server_address[1],"encoded":len(zipped),
                      "decoded":len(raw),"sha256":hashlib.sha256(raw).hexdigest()}), flush=True)
    server.serve_forever()

def client(args):
    sys.path.insert(0, str((ROOT / ("_ab_base" if args.version == "base" else "_ab_candidate") / "src").resolve()))
    import urllib3, urllib3.response
    target = (ROOT / ("_ab_base" if args.version == "base" else "_ab_candidate") / "src").resolve()
    assert pathlib.Path(urllib3.response.__file__).resolve().is_relative_to(target), urllib3.response.__file__
    metric = {"decode_calls":0,"one_chunk":0,"multi_chunk":0,"empty":0}
    # Coverage runs are deliberately separate; instrumentation never contaminates timed results.
    if args.probe == "coverage" and args.kind != "plain":
        Original = urllib3.response.GzipDecoder
        class Wrapped:
            def __init__(self, inner, tally): self.inner,self.tally = inner,tally
            def decompress(self,*a,**kw):
                out=self.inner.decompress(*a,**kw)
                if out: self.tally[0]+=1
                return out
            def __getattr__(self,n): return getattr(self.inner,n)
        class Counting(Original):
            def decompress(self,data,max_length=-1):
                tally=[0]; original=self._obj
                self._obj=Wrapped(original,tally)
                try: result=super().decompress(data,max_length)
                finally:
                    if isinstance(self._obj,Wrapped): self._obj=self._obj.inner
                metric["decode_calls"]+=1
                metric["one_chunk" if tally[0]==1 else ("empty" if tally[0]==0 else "multi_chunk")]+=1
                return result
        urllib3.response.GzipDecoder=Counting
    pool=urllib3.PoolManager(maxsize=args.concurrency,block=True,retries=False,
            timeout=urllib3.Timeout(connect=5,read=45))
    url=f"http://127.0.0.1:{args.port}/"
    def request(i):
        if args.mode=="stream":
            response=pool.request("GET",url,preload_content=False)
            try:
                size=0
                for chunk in response.stream(amt=65536,decode_content=True): size+=len(chunk)
            finally: response.release_conn()
        elif args.mode=="read":
            response=pool.request("GET",url,preload_content=False)
            try: size=len(response.read(decode_content=True))
            finally: response.release_conn()
        else:
            response=pool.request("GET",url,preload_content=True,decode_content=True)
            size=len(response.data)
        if size!=args.expected: raise RuntimeError(f"response bytes mismatch {size} vs {args.expected}")
        return size
    def batch(count):
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
            return sum(ex.map(request,range(count)))
    try:
        batch(max(args.concurrency,2)) # cache and connection warmup
        metric={k:0 for k in metric}
        # Explicit correctness probe outside timed region.
        if args.probe=="correctness":
            resp=pool.request("GET",url,decode_content=True)
            expected=hashlib.sha256(data_for(args.kind,args.mib)).hexdigest()
            assert hashlib.sha256(resp.data).hexdigest()==expected
        proc=psutil.Process()
        rss=[]; stop=threading.Event()
        def sample():
            while not stop.wait(0.005):
                rss.append(proc.memory_info().rss)
        if args.probe=="alloc": tracemalloc.start()
        watcher=threading.Thread(target=sample,daemon=True);watcher.start()
        gc0=gc.get_count()
        cpu0=time.process_time(); wall0=time.perf_counter()
        total=batch(args.requests)
        wall=time.perf_counter()-wall0; cpu=time.process_time()-cpu0
        stop.set();watcher.join(timeout=2)
        peak_trace=tracemalloc.get_traced_memory()[1] if args.probe=="alloc" else None
        if args.probe=="alloc":tracemalloc.stop()
        print(json.dumps({"version":args.version,"scenario":args.scenario,
            "wall_s":wall,"cpu_s":cpu,"mib_s":total/1048576/wall,
            "cpu_s_per_gib":cpu/(total/(1024**3)),
            "rss_max_mib":max(rss+[proc.memory_info().rss])/1048576,
            "alloc_peak_mib":peak_trace/1048576 if peak_trace is not None else None,
            "gc_delta":[b-a for a,b in zip(gc0,gc.get_count())],
            "coverage":metric if args.probe=="coverage" else None,
            "source":urllib3.response.__file__}),flush=True)
    finally: pool.clear()

def orchestration(args):
    OUT.mkdir(exist_ok=True)
    import platform
    meta={"base":BASE,"candidate":NEW,"python":sys.version,"platform":platform.platform(),
          "processor":platform.processor(),"logical_cpu":psutil.cpu_count(),
          "physical_cpu":psutil.cpu_count(logical=False),"ram_mib":psutil.virtual_memory().total/1048576,
          "runner":os.environ.get("RUNNER_NAME"),"warning":"controlled loopback fixture, not production mix"}
    scenarios=[
        ("compressible",1,1,"preload"),("compressible",1,8,"preload"),
        ("compressible",8,1,"preload"),("compressible",8,8,"preload"),
        ("compressible",32,1,"preload"),("compressible",32,8,"preload"),
        ("incompressible",8,1,"preload"),("incompressible",8,8,"preload"),
        ("incompressible",32,1,"preload"),("incompressible",32,8,"preload"),
        ("compressible",8,8,"stream"),("compressible",32,8,"stream"),
        ("compressible",8,8,"read"),("json",1,8,"preload"),
        ("plain",8,8,"preload")
    ]
    raw=[];summary=[]
    def save():
        (OUT/"raw.json").write_text(json.dumps({"metadata":meta,"measurements":raw},indent=2)+"\n")
        (OUT/"summary.json").write_text(json.dumps({"metadata":meta,"summary":summary},indent=2)+"\n")
    for kind,mib,threads,mode in scenarios:
        key=f"{kind}/{mib}MiB/c{threads}/{mode}"
        server=subprocess.Popen([sys.executable,__file__,"--server","--kind",kind,"--mib",str(mib)],
                                stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1)
        try:
            info=json.loads(server.stdout.readline())
            def call(version,probe="none"):
                source=ROOT/("_ab_base" if version=="base" else "_ab_candidate")/"src"
                n=max(12,threads*2) if mib<32 else max(8,threads)
                cmd=[sys.executable,__file__,"--client","--version",version,"--kind",kind,
                     "--mib",str(mib),"--port",str(info["port"]),"--expected",str(info["decoded"]),
                     "--mode",mode,"--concurrency",str(threads),"--requests",str(n),
                     "--probe",probe,"--scenario",key]
                t=subprocess.run(cmd,text=True,capture_output=True,timeout=150,cwd=ROOT)
                if t.returncode:raise RuntimeError(f"{key}: {t.stderr[-3000:]}")
                return json.loads(t.stdout.strip().splitlines()[-1])
            for rnd in range(args.rounds):
                for version in (["base","candidate"] if rnd%2==0 else ["candidate","base"]):
                    item=call(version);item["round"]=rnd;raw.append(item)
                save()
            coverage=call("candidate","coverage")
            alloc_base=call("base","alloc")
            alloc_new=call("candidate","alloc")
            correctness=call("candidate","correctness")
            assert correctness["wall_s"]>0
            base=[x for x in raw if x["scenario"]==key and x["version"]=="base" and "round" in x]
            new=[x for x in raw if x["scenario"]==key and x["version"]=="candidate" and "round" in x]
            pairs=[{"throughput_pct":100*(a["mib_s"]/b["mib_s"]-1),
                    "cpu_saved_pct":100*(1-a["cpu_s"]/b["cpu_s"])}
                    for b,a in zip(base,new)]
            med=lambda data,k:statistics.median(x[k] for x in data)
            item={"scenario":key,"throughput_pct":med(pairs,"throughput_pct"),
                  "cpu_saved_pct":med(pairs,"cpu_saved_pct"),
                  "cpu_s_per_gib_baseline":med(base,"cpu_s_per_gib"),
                  "cpu_s_per_gib_candidate":med(new,"cpu_s_per_gib"),
                  "rss_delta_mib":med(new,"rss_max_mib")-med(base,"rss_max_mib"),
                  "alloc_delta_mib":alloc_new["alloc_peak_mib"]-alloc_base["alloc_peak_mib"],
                  "coverage":coverage["coverage"],"pair_results":pairs}
            summary.append(item);save()
            print(json.dumps(item),flush=True)
        finally:
            server.terminate()
            try:server.communicate(timeout=5)
            except subprocess.TimeoutExpired:server.kill();server.communicate()
    print("COMPLETED",len(summary),"scenarios",flush=True)

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--server",action="store_true");p.add_argument("--client",action="store_true")
    p.add_argument("--kind",default="compressible");p.add_argument("--mib",type=int,default=8)
    p.add_argument("--version",default="base");p.add_argument("--port",type=int,default=0)
    p.add_argument("--expected",type=int,default=0);p.add_argument("--concurrency",type=int,default=8)
    p.add_argument("--mode",default="preload");p.add_argument("--probe",default="none")
    p.add_argument("--scenario",default="");p.add_argument("--requests",type=int,default=12)
    p.add_argument("--rounds",type=int,default=4)
    a=p.parse_args()
    if a.server:serve(a)
    elif a.client:client(a)
    else:orchestration(a)
if __name__=="__main__":main()
