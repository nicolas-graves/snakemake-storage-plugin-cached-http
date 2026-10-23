# SPDX-License-Identifier: MIT
"""Real local HTTP, independent processes, and Snakemake runtime integration."""

import asyncio
import logging
import multiprocessing
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from snakemake_storage_plugin_cached_http import (
    StorageProvider,
    StorageProviderSettings,
)


@pytest.fixture
def server():
    counts = {"GET": 0, "HEAD": 0}

    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            counts["HEAD"] += 1
            self.send_response(200)
            self.send_header("Content-Length", "6")
            self.send_header("ETag", '"v1"')
            self.end_headers()

        def do_GET(self):
            counts["GET"] += 1
            self.send_response(200)
            self.send_header("Content-Length", "6")
            self.send_header("ETag", '"v1"')
            self.end_headers()
            self.wfile.write(b"public")

        def log_message(self, *args):
            pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{http.server_port}/data?geo=FR", counts
    http.shutdown()
    thread.join()
    http.server_close()


def _download_worker(directory, url, number):
    root = Path(directory)
    p = StorageProvider(
        local_prefix=root / "stage",
        logger=logging.getLogger("test"),
        settings=StorageProviderSettings(
            cache=str(root / "cache"), public_cache=True, policy="immutable"
        ),
    )
    asyncio.run(p.fetch(url, root / f"out-{number}"))


def test_processes_download_once(server, tmp_path):
    url, counts = server
    context = multiprocessing.get_context("spawn")
    workers = [
        context.Process(target=_download_worker, args=(str(tmp_path), url, i))
        for i in range(3)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(15)
        assert worker.exitcode == 0
    assert counts == {"GET": 1, "HEAD": 0}
    assert all((tmp_path / f"out-{i}").read_bytes() == b"public" for i in range(3))


@pytest.mark.parametrize("execution", ["shell", "run"])
def test_snakemake_dryrun_dag_retrieve_cleanup_rerun(server, tmp_path, execution):
    url, counts = server
    cache = tmp_path / "cache"
    execute = ('shell: "cp {input} {output}"' if execution == "shell" else
               'run:\n        import shutil\n        shutil.copyfile(input[0], output[0])')
    snakefile = tmp_path / "Snakefile"
    snakefile.write_text(f"""
storage cached_http:
    provider="cached-http",
    cache={str(cache)!r},
    public_cache=True,
    headers={{"Accept": "application/json"}},
    policy="immutable",

rule all:
    input: "result"

rule download:
    input: storage.cached_http({url!r})
    output: "result"
    {execute}
""")

    def run(*arguments):
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "snakemake",
                "--profile",
                "none",
                "--workflow-profile",
                "none",
                "--scheduler",
                "greedy",
                "--cores",
                "1",
                *arguments,
            ],
            cwd=tmp_path,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        assert result.returncode == 0, result.stdout
        return result.stdout

    run("--dry-run")
    assert counts["GET"] == 0
    run("--dag")
    assert counts["GET"] == 0
    run()
    assert (tmp_path / "result").read_bytes() == b"public"
    assert counts["GET"] == 1
    # Snakemake cleanup/local staging deletion must not remove persistent bytes.
    import shutil

    shutil.rmtree(tmp_path / ".snakemake" / "storage", ignore_errors=True)
    (tmp_path / "result").unlink()
    run()
    assert counts["GET"] == 1
    run("--dry-run")
    assert counts["GET"] == 1
    assert "Nothing to be done" in run()


def test_rejected_body_is_not_reused_by_next_run(tmp_path):
    # A maintenance page served once must not stick under immutable policy.
    bodies = [b"<!DOCTYPE html><title>Maintenance en cours</title>", b"a,b\n1,2\n"]
    gets = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = bodies[min(len(gets), 1)]
            gets.append(self.path)
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{http.server_port}/workbook.csv"
    (tmp_path / "Snakefile").write_text(f"""
import os

storage cached_http:
    provider="cached-http",
    cache=os.environ["SNAKEMAKE_STORAGE_CACHED_HTTP_CACHE"],
    public_cache=True,
    policy="immutable",
    skip_remote_checks=True,

rule download:
    input: source=storage.cached_http({url!r})
    output: "result.csv"
    run:
        from snakemake_storage_plugin_cached_http.validation import materialize_validated
        materialize_validated(input.source, output[0], reject_api_errors=True)
""")
    environment = {
        **os.environ,
        "SNAKEMAKE_STORAGE_CACHED_HTTP_CACHE": str(tmp_path / "cache"),
    }

    def run():
        return subprocess.run(
            [sys.executable, "-m", "snakemake", "--profile", "none",
             "--workflow-profile", "none", "--scheduler", "greedy", "--cores", "1"],
            cwd=tmp_path, env=environment, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )

    try:
        first = run()
        assert first.returncode != 0
        assert "HTML response where source data was expected" in first.stdout
        second = run()
        assert second.returncode == 0, second.stdout
    finally:
        http.shutdown()
        thread.join()
        http.server_close()
    assert (tmp_path / "result.csv").read_bytes() == b"a,b\n1,2\n"
    assert len(gets) == 2
