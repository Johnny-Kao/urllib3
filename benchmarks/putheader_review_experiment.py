from __future__ import annotations

import gc
import http.client
import json
import random
import statistics
import sys
import time

from http.client import HTTPConnection as _HTTPConnection

from urllib3.connection import HTTPConnection
from urllib3.util import SKIP_HEADER, SKIPPABLE_HEADERS
from urllib3.util.util import to_str


def _unsupported_skip(header: str) -> None:
    skippable_headers = "', '".join(
        [str.title(name) for name in sorted(SKIPPABLE_HEADERS)]
    )
    raise ValueError(
        f"urllib3.util.SKIP_HEADER only supports '{skippable_headers}'"
    )


def baseline_putheader(self: HTTPConnection, header: str, *values: str) -> None:
    if not any(isinstance(v, str) and v == SKIP_HEADER for v in values):
        _HTTPConnection.putheader(self, header, *values)
    elif to_str(header.lower()) not in SKIPPABLE_HEADERS:
        _unsupported_skip(header)


def original_pr_putheader(self: HTTPConnection, header: str, *values: str) -> None:
    if len(values) == 1:
        value = values[0]
        skip_header = isinstance(value, str) and value == SKIP_HEADER
    else:
        skip_header = any(
            isinstance(value, str) and value == SKIP_HEADER for value in values
        )

    if not skip_header:
        _HTTPConnection.putheader(self, header, *values)
    elif to_str(header.lower()) not in SKIPPABLE_HEADERS:
        _unsupported_skip(header)


def reviewer_putheader(self: HTTPConnection, header: str, *values: str) -> None:
    if len(values) == 1:
        value = values[0]
        if not (isinstance(value, str) and value == SKIP_HEADER):
            _HTTPConnection.putheader(self, header, value)
            return
        skip_header = True
    else:
        skip_header = any(
            isinstance(value, str) and value == SKIP_HEADER for value in values
        )

    if not skip_header:
        _HTTPConnection.putheader(self, header, *values)
    elif to_str(header.lower()) not in SKIPPABLE_HEADERS:
        _unsupported_skip(header)


VARIANTS = {
    "A_baseline": baseline_putheader,
    "B_original_pr": original_pr_putheader,
    "C_reviewer": reviewer_putheader,
}


def make_case(header_count: int):
    conn = HTTPConnection("example.com")
    headers = [(f"X-Bench-{i}", f"value-{i}") for i in range(header_count)]

    def run_once() -> None:
        # Isolate header construction without network I/O. stdlib putheader()
        # still performs its normal encoding/validation and appends to _buffer.
        conn._HTTPConnection__state = http.client._CS_REQ_STARTED
        conn._buffer = []
        for header, value in headers:
            conn.putheader(header, value)

    return conn, run_once


def measure_case(header_count: int, *, samples: int = 31, loops: int = 20000):
    conns = {}
    runners = {}
    for name in VARIANTS:
        conn, runner = make_case(header_count)
        conns[name] = conn
        runners[name] = runner

    # Warm all variants before collecting data.
    for name, impl in VARIANTS.items():
        HTTPConnection.putheader = impl  # type: ignore[method-assign]
        for _ in range(3000):
            runners[name]()

    observations = {name: [] for name in VARIANTS}
    rng = random.Random(5287 + header_count)

    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        for _sample in range(samples):
            order = list(VARIANTS)
            rng.shuffle(order)
            for name in order:
                HTTPConnection.putheader = VARIANTS[name]  # type: ignore[method-assign]
                runner = runners[name]
                start = time.perf_counter_ns()
                for _ in range(loops):
                    runner()
                elapsed = time.perf_counter_ns() - start
                observations[name].append(elapsed / loops)
    finally:
        if gc_was_enabled:
            gc.enable()

    medians = {name: statistics.median(v) for name, v in observations.items()}
    q = {
        name: statistics.quantiles(v, n=4, method="inclusive")
        for name, v in observations.items()
    }

    a = medians["A_baseline"]
    b = medians["B_original_pr"]
    c = medians["C_reviewer"]

    return {
        "headers": header_count,
        "samples": samples,
        "loops_per_sample": loops,
        "median_ns_per_batch": medians,
        "iqr_ns_per_batch": {
            name: [quartiles[0], quartiles[2]] for name, quartiles in q.items()
        },
        "speedup_percent": {
            "B_vs_A": (a / b - 1.0) * 100.0,
            "C_vs_B": (b / c - 1.0) * 100.0,
            "C_vs_A": (a / c - 1.0) * 100.0,
        },
        "time_reduction_percent": {
            "B_vs_A": (1.0 - b / a) * 100.0,
            "C_vs_B": (1.0 - c / b) * 100.0,
            "C_vs_A": (1.0 - c / a) * 100.0,
        },
    }


def main() -> None:
    original = HTTPConnection.putheader
    try:
        results = [measure_case(n) for n in (1, 4, 12)]
    finally:
        HTTPConnection.putheader = original  # type: ignore[method-assign]

    payload = {
        "python": sys.version,
        "platform": sys.platform,
        "commit_mapping": {
            "A_baseline": "8b05e57c47f7f2d17eaea0b9ada1fc1b85255550",
            "B_original_pr": "30bde008562e603b6de4fd7dd588df94c6ad40b7",
            "C_reviewer": "a5ccd51bff91b5b67aa825b67b550446085821b4",
        },
        "method": "same-process interleaved benchmark; real stdlib HTTPConnection.putheader; no network",
        "results": results,
    }
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
