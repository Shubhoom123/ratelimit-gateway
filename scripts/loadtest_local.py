#!/usr/bin/env python3
"""Local load-test harness: starts Redis, a controllable backend and two gateway
instances, runs the scenarios with cmd/loadgen, and writes results/*.json plus
docs/loadtest-report.md.

Everything shares this machine's CPUs (including the load generator), so the
absolute numbers are a floor for the code, not a capacity claim. The AWS run
(k6 scripts in loadtest/k6) is the one to quote for capacity.

    make build && python3 scripts/loadtest_local.py
"""
import json, os, signal, subprocess, sys, tempfile, threading, time, urllib.request, platform

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "bin")
RES = os.path.join(ROOT, "results")
os.makedirs(RES, exist_ok=True)

REDIS_PORT, BACKEND_PORT = 6390, 9001
GW = [(8081, 9091), (8082, 9092)]  # (proxy, admin)
CFG = os.path.join(RES, "config.json")
procs = {}


def sh(*a, **k):
    return subprocess.Popen(list(a), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **k)


def http(url, timeout=3):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read().decode()


def wait_for(fn, what, secs=10):
    end = time.time() + secs
    while time.time() < end:
        try:
            if fn():
                return
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"timeout waiting for {what}")


def start_redis():
    procs["redis"] = sh("redis-server", "--port", str(REDIS_PORT), "--save", "", "--appendonly", "no")
    wait_for(lambda: subprocess.run(["redis-cli", "-p", str(REDIS_PORT), "ping"], capture_output=True).stdout.strip() == b"PONG", "redis")


def stop_redis():
    subprocess.run(["redis-cli", "-p", str(REDIS_PORT), "shutdown", "nosave"], capture_output=True)
    procs.pop("redis", None)


def start_backend():
    procs["backend"] = sh(f"{BIN}/backend", "-listen", f":{BACKEND_PORT}", "-latency", "5ms", "-workers", "64")
    wait_for(lambda: http(f"http://127.0.0.1:{BACKEND_PORT}/__control"), "backend")


def stop_backend():
    p = procs.pop("backend", None)
    if p:
        p.kill()
        p.wait()


def control(**kw):
    q = "&".join(f"{k}={v}" for k, v in kw.items())
    http(f"http://127.0.0.1:{BACKEND_PORT}/__control?{q}")


def limit(alg, n=200, burst=None):
    return {"algorithm": alg, "limit": n, "window": "1s", "burst": burst or n}


def make_config(fast_path=False, version=0):
    be = f"http://127.0.0.1:{BACKEND_PORT}"
    brk = {"window": 20, "min_requests": 20, "failure_ratio": 0.5, "open_for": "2s", "probes": 3}
    never = {"window": 20, "min_requests": 1000000, "failure_ratio": 1.0, "open_for": "1s", "probes": 1}
    route = lambda name, **kw: {"name": name, "prefix": "/" + name, "backend": "svc", **kw}
    return {
        "redis": {"addr": f"127.0.0.1:{REDIS_PORT}", "prefix": f"lt{version}", "pool_size": 128, "timeout": "25ms",
                  "breaker": {"window": 20, "min_requests": 10, "failure_ratio": 0.5, "open_for": "1s", "probes": 3}},
        "defaults": limit("token_bucket", 10000000),
        "on_redis_error": "local_fallback", "fallback_divisor": 2,
        "fast_path": {"enabled": fast_path},
        "backends": {
            "svc": {"url": be, "max_concurrent": 512, "queue_wait": "25ms", "breaker": never},
            "protected": {"url": be, "max_concurrent": 50, "queue_wait": "10ms", "breaker": brk},
            "unprotected": {"url": be, "max_concurrent": 100000, "queue_wait": "10s", "breaker": never},
        },
        "routes": [
            route("api"),
            route("tb", limit=limit("token_bucket")),
            route("tbf", limit=limit("token_bucket")),
            route("swl", limit=limit("sliding_window_log")),
            route("swc", limit=limit("sliding_window_counter")),
            {"name": "prot", "prefix": "/prot", "backend": "protected", "timeout": "250ms"},
            {"name": "unprot", "prefix": "/unprot", "backend": "unprotected", "timeout": "5s"},
            {"name": "dead", "prefix": "/dead", "backend": "protected", "timeout": "250ms"},
            route("rd_local", limit=limit("token_bucket", 500), on_redis_error="local_fallback"),
            route("rd_open", limit=limit("token_bucket", 500), on_redis_error="fail_open"),
            route("rd_closed", limit=limit("token_bucket", 500), on_redis_error="fail_closed"),
        ],
    }


cfg_version = [0]


def apply_config(**kw):
    cfg_version[0] += 1
    c = make_config(version=cfg_version[0], **kw)
    tmp = CFG + ".tmp"
    json.dump(c, open(tmp, "w"), indent=1)
    os.replace(tmp, CFG)  # atomic, like a real config push
    # wait until both gateways report a new version via /admin/config
    def reloaded():
        vs = {json.loads(http(f"http://127.0.0.1:{a}/admin/config"))["version"] for _, a in GW}
        return len(vs) == 1 and vs != apply_config.last
    wait_for(reloaded, "config reload", 15)
    apply_config.last = {json.loads(http(f"http://127.0.0.1:{GW[0][1]}/admin/config"))["version"]}


apply_config.last = set()


def start_gateways():
    for i, (port, admin) in enumerate(GW):
        procs[f"gw{i}"] = sh(f"{BIN}/gateway", "-listen", f":{port}", "-admin", f":{admin}", "-config", CFG, "-reload-interval", "300ms")
    for _, admin in GW:
        wait_for(lambda a=admin: "ready" in http(f"http://127.0.0.1:{a}/readyz"), "gateway", 15)
    apply_config.last = {json.loads(http(f"http://127.0.0.1:{GW[0][1]}/admin/config"))["version"]}


def redis_calls():
    total = 0
    for _, admin in GW:
        for line in http(f"http://127.0.0.1:{admin}/metrics").splitlines():
            if line.startswith("gateway_redis_calls "):
                total += float(line.split()[1])
    return total


def run(name, path, rate, dur, extra=(), events=(), timeline=False, both=True):
    urls = ",".join(f"http://127.0.0.1:{p}{path}" for p, _ in (GW if both else GW[:1]))
    out = os.path.join(RES, f"{name}.json")
    cmd = [f"{BIN}/loadgen", "-url", urls, "-rate", str(rate), "-duration", f"{dur}s", "-name", name, "-out", out, *extra]
    if timeline:
        cmd.append("-timeline")
    threads = []
    for at, fn in events:
        t = threading.Timer(at, fn)
        t.start()
        threads.append(t)
    before = redis_calls() if redis_ok() else None
    subprocess.run(cmd, stdout=subprocess.DEVNULL, check=True)
    for t in threads:
        t.join()
    r = json.load(open(out))
    if before is not None and redis_ok():
        r["redis_calls"] = redis_calls() - before
    print(f"  {name}: {r['achieved_rps']:.0f} rps, allowed {r['allowed_rps']:.0f}/s, p99 {r['p99_ms']:.1f}ms codes {r['status_codes']}", flush=True)
    json.dump(r, open(out, "w"), indent=1)
    return r


def redis_ok():
    return subprocess.run(["redis-cli", "-p", str(REDIS_PORT), "ping"], capture_output=True).stdout.strip() == b"PONG"


def cleanup(*_):
    for p in list(procs.values()):
        try:
            p.kill()
        except Exception:
            pass
    subprocess.run(["redis-cli", "-p", str(REDIS_PORT), "shutdown", "nosave"], capture_output=True)


def main():
    for b in ("gateway", "backend", "loadgen"):
        if not os.path.exists(f"{BIN}/{b}"):
            sys.exit(f"missing bin/{b}; run `make build`")
    signal.signal(signal.SIGINT, lambda *a: (cleanup(), sys.exit(1)))
    only = {a.lower() for a in sys.argv[1:]}
    want = lambda n: not only or n in only
    summary_path = os.path.join(RES, "summary.json")
    R = json.load(open(summary_path)) if only and os.path.exists(summary_path) else {}
    try:
        start_redis(); start_backend()
        json.dump(make_config(version=0), open(CFG, "w"))
        start_gateways()
        time.sleep(1)

        if want("s1"):
            print("S1 baseline throughput and latency (no limiting, backend 5ms)")
            R["baseline_1000"] = run("baseline_1000", "/api", 1000, 10)
            R["baseline_2000"] = run("baseline_2000", "/api", 2000, 10)

        if want("s2"):
            print("S2 limiter accuracy: one identity, 2 gateway instances, 600 rps offered vs 200/s limit, 10s")
            for alg, p in (("token_bucket", "tb"), ("sliding_window_log", "swl"), ("sliding_window_counter", "swc")):
                R[f"acc_{p}"] = run(f"acc_{p}", f"/{p}", 600, 10)
                time.sleep(3)

        if want("s3"):
            print("S3 local fast path: same traffic as S2 token bucket")
            apply_config(fast_path=True)
            R["acc_tbf"] = run("acc_tbf", "/tbf", 600, 10)
            R["baseline_fast_2000"] = run("baseline_fast_2000", "/api", 2000, 10)
            apply_config(fast_path=False)
            time.sleep(3)

        if want("s4"):
            print("S4 backend slows from 5ms to 400ms at t=5s and recovers at t=13s (800 rps, 20s)")
            for route in ("unprot", "prot"):
                R[f"slow_{route}"] = run(f"slow_{route}", f"/{route}", 800, 20, timeline=True,
                                         events=[(5, lambda: control(latency="400ms")), (13, lambda: control(latency="5ms"))])
                control(latency="5ms"); time.sleep(8)

        if want("s5"):
            print("S5 backend killed at t=5s, restarted at t=12s (800 rps, 20s)")
            R["kill_backend"] = run("kill_backend", "/dead", 800, 20, timeline=True,
                                    events=[(5, stop_backend), (12, start_backend)])
            time.sleep(3)

        if want("s6"):
            print("S6 Redis killed at t=5s, restarted at t=12s (1500 rps offered vs 500/s limit, 20s)")
            for pol in ("rd_local", "rd_open", "rd_closed"):
                def restart():
                    start_redis()
                R[f"redis_down_{pol}"] = run(f"redis_down_{pol}", f"/{pol}", 1500, 20, timeline=True,
                                             events=[(5, stop_redis), (12, restart)])
                time.sleep(4)
    finally:
        cleanup()
    json.dump(R, open(os.path.join(RES, "summary.json"), "w"), indent=1)
    print("done ->", RES)


if __name__ == "__main__":
    main()
