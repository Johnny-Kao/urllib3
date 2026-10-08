#!/usr/bin/env python3
"""Controlled loopback HTTP A/B benchmark for urllib3 PR #5304.

Not a production traffic measurement. Both versions run on the same runner,
identical Python/dependencies/workload, in separate processes, in ABBA order.
"""
import argparse
import concurrent.futures
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import resource
import statistics
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE = "a164d79c8cf760f222daa2dc7f67d0e1ca7fb17c"
CANDIDATE = "6ae668b355863f72d4381c4696c4748cd386a7d6"


def make_payload(size, kind):
    if kind == "compressible":
        return (b"0123456789abcdef" * ((size + 15) // 16))[:size]
    return random.Random(41017).randbytes(size)


def run_worker(args):
    # Ensure this Python process loads exactly one version.
    sys.path.insert(0, str(Path(args.source).resolve() / "src"))
    import urllib3
    import urllib3.response
    import psutil
    actual = str(Path(urllib3.response.__file__).resolve())
    expected = str(Path(args.source).resolve() / "src")
    assert actual.startswith(expected + os.sep), (actual, expected)
    payload = make_payload(args.size_mib * 1048576, args.kind)
    compressed = gzip.compress(payload, compresslevel=6, mtime=0)
    digest = hashlib.sha256(payload).digest()
    counters = {"decoder_calls": 0, "fastpath_calls": 0, "nonempty_chunks": 0}
    if args.coverage:
        Original = urllib3.response.GzipDecoder

        class Proxy:
            def __init__(self, obj, counter):
                self.obj, self.counter = obj, counter

            def decompress(self, *a, **kw):
                out = self.obj.decompress(*a, **kw)
                if out:
                    self.counter[0] += 1
                return out

            def __getattr__(self, name):
                return getattr(self.obj, name)

        class Instrumented(Original):
            def decompress(self, data, max_length=-1):
                n = [0]
                old = self._obj
                self._obj = Proxy(old, n)
                try:
                    result = super().decompress(data, max_length)
                finally:
                    if isinstance(self._obj, Proxy):
                        self._obj = self._obj.obj
                counters["decoder_calls"] += 1
                counters["nonempty_chunks"] += n[0]
                if n[0] == 1:
                    counters["fastpath_calls"] += 1
                return result

        urllib3.response.GzipDecoder = Instrumented

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(compressed)))
            self.end_headers()
            self.wfile.write(compressed)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    pool = urllib3.PoolManager(maxsize=args.concurrency, block=True, retries=False, timeout=urllib3.Timeout(connect=5.0, read=30.0))
    url = f"http://127.0.0.1:{server.server_address[1]}/data"
    process = psutil.Process()
    stop = threading.Event()
    samples = []

    def sample():
        while not stop.wait(0.01):
            samples.append(process.memory_info().rss)

    def request(_):
        response = pool.request("GET", url, preload_content=True, decode_content=True)
        if response.status != 200 or len(response.data) != len(payload) or hashlib.sha256(response.data).digest() != digest:
            raise RuntimeError("HTTP payload mismatch")
        return len(response.data)

    try:
        # Warm up sockets, imports, decompressor and server threads before measurement.
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
            list(ex.map(request, range(min(4, args.concurrency))))
        if args.coverage:
            counters = {k: 0 for k in counters}
        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        cpu0, wall0 = time.process_time(), time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
            sizes = list(ex.map(request, range(args.requests)))
        wall_s = time.perf_counter() - wall0
        cpu_s = time.process_time() - cpu0
        stop.set()
        sampler.join(timeout=2)
        maxrss = max(samples + [process.memory_info().rss])
        result = {
            "version": args.version,
            "kind": args.kind, "size_mib": args.size_mib,
            "concurrency": args.concurrency, "requests": args.requests,
            "wall_seconds": wall_s, "process_cpu_seconds": cpu_s,
            "throughput_mib_s": sum(sizes) / 1048576 / wall_s,
            "requests_per_s": len(sizes) / wall_s,
            "peak_rss_mib": maxrss / 1048576,
            "cpu_cores_effective": cpu_s / wall_s,
            "compressed_bytes": len(compressed),
            "decoded_bytes": len(payload),
            "source_file": actual, "urllib3_version": urllib3.__version__,
            "coverage": dict(counters) if args.coverage else None,
        }
        print(json.dumps(result), flush=True)
    finally:
        pool.clear()
        server.shutdown()
        server.server_close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--source", default="")
    parser.add_argument("--version", default="")
    parser.add_argument("--kind", default="compressible")
    parser.add_argument("--size-mib", type=int, default=8)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--requests", type=int, default=24)
    parser.add_argument("--coverage", action="store_true")
    parser.add_argument("--rounds", type=int, default=2)
    args = parser.parse_args()
    if args.worker:
        run_worker(args)
        return

    import psutil
    root = Path(__file__).resolve().parents[1]
    base, candidate = root / "_ab_base", root / "_ab_candidate"
    assert base.exists() and candidate.exists()
    outdir = root / "ab-results"
    outdir.mkdir(exist_ok=True)
    metadata = {
        "baseline_sha": BASE, "candidate_sha": CANDIDATE,
        "platform": platform.platform(), "python": sys.version,
        "cpu": platform.processor(), "logical_cores": psutil.cpu_count(),
        "physical_cores": psutil.cpu_count(logical=False),
        "ram_total_mib": psutil.virtual_memory().total / 1048576,
        "ci_runner": os.environ.get("RUNNER_NAME", ""),
        "ci_os": os.environ.get("RUNNER_OS", ""),
        "psutil_version": psutil.__version__,
        "note": "loopback HTTP client + server, NOT public-network end-to-end nor production fastpath prevalence",
    }
    scenarios = [(k, s, c) for k in ("compressible", "incompressible") for s in (1, 8, 32) for c in (1, 8)]
    results = []

    def launch(version, kind, size, concurrency, coverage=False):
        source = base if version == "base" else candidate
        cmd = [sys.executable, str(Path(__file__).resolve()), "--worker",
               "--source", str(source), "--version", version, "--kind", kind,
               "--size-mib", str(size), "--concurrency", str(concurrency),
               "--requests", str(max(12, concurrency * 3))]
        if coverage:
            cmd.append("--coverage")
        p = subprocess.run(cmd, cwd=root, capture_output=True, text=True, timeout=180)
        if p.returncode:
            raise RuntimeError(f"{cmd}\n{p.stdout}\n{p.stderr}")
        return json.loads(p.stdout.strip().splitlines()[-1])

    for kind, size, concurrency in scenarios:
        for rnd in range(args.rounds):
            # Alternating AB / BA; same machine, all paired; no parallel A vs B.
            order = ("base", "candidate") if rnd % 2 == 0 else ("candidate", "base")
            for version in order:
                item = launch(version, kind, size, concurrency)
                item["round"] = rnd
                results.append(item)
                print(f"{kind} {size}MiB c={concurrency} r={rnd} {version}: "
                      f"{item['throughput_mib_s']:.1f} MiB/s, "
                      f"CPU {item['process_cpu_seconds']:.3f}s, RSS {item['peak_rss_mib']:.1f} MiB",
                      flush=True)
        # Coverage probe separate from timing to avoid instrumentation contamination.
        results.append(launch("candidate", kind, size, concurrency, coverage=True))

    (outdir / "raw.json").write_text(json.dumps({"metadata": metadata, "results": results}, indent=2) + "\n")
    summary = []
    for kind, size, concurrency in scenarios:
        group = [r for r in results if r["kind"] == kind and r["size_mib"] == size
                 and r["concurrency"] == concurrency and r["coverage"] is None]
        b = [r for r in group if r["version"] == "base"]
        a = [r for r in group if r["version"] == "candidate"]
        med = lambda seq, key: statistics.median(x[key] for x in seq)
        cov = next(r["coverage"] for r in results if r["coverage"] is not None
                   and r["kind"] == kind and r["size_mib"] == size and r["concurrency"] == concurrency)
        summary.append({
            "scenario": f"{kind}/{size}MiB/c{concurrency}",
            "throughput_change_pct": 100 * (med(a, "throughput_mib_s") / med(b, "throughput_mib_s") - 1),
            "cpu_saved_pct": 100 * (1 - med(a, "process_cpu_seconds") / med(b, "process_cpu_seconds")),
            "rss_change_mib": med(a, "peak_rss_mib") - med(b, "peak_rss_mib"),
            "fastpath_hit_rate_pct": 100 * cov["fastpath_calls"] / max(1, cov["decoder_calls"]),
            "fastpath_numerator": cov["fastpath_calls"], "fastpath_denominator": cov["decoder_calls"],
        })
    (outdir / "summary.json").write_text(json.dumps({"metadata": metadata, "summary": summary}, indent=2) + "\n")
    print("A/B SUMMARY", flush=True)
    for item in summary:
        print(json.dumps(item), flush=True)


if __name__ == "__main__":
    main()
