"""Pulls complete exports (vN/ with _SUCCESS) into SERVING_DIR/vN/ (CONTEXT §5 step 5).

  python puller.py                 # poll every 10 s, POST /reload to the local API after each pull
  python puller.py --once          # one check and exit
  python puller.py --version 1     # manual pull of one version (also retries one that failed before)
  python puller.py --source file:///tmp/fake_export --serving /tmp/serving --once   # no cluster

The API (app.py) runs the same sync() in a background thread, so this CLI is only needed for
manual pulls. Download goes to vN.tmp/, is validated against the manifest, and only then renamed
to vN/ (atomic on one filesystem). Any failure deletes vN.tmp/ and leaves what is served untouched.
A vN that was re-exported (export.py --force after a rollback) has a new manifest and is pulled again.

Env: EXPORT_SOURCE (default http://node2:9870/searchengine/export), SERVING_DIR (default /serving).
"""
import argparse
import json
import logging
import os
import re
import shutil
import time
from pathlib import Path
from urllib.parse import urlparse

import requests

from search_index import InvalidExport, read_export, read_manifest

EXPORT_SOURCE = os.environ.get("EXPORT_SOURCE", "http://node2:9870/searchengine/export")
SERVING_DIR = os.environ.get("SERVING_DIR", "/serving")
VERSION_DIR = re.compile(r"^v(\d+)$")
TABLES = ("documents", "postings")

log = logging.getLogger("puller")


class WebHdfsSource:
    """Plain HTTP to the NameNode; OPEN redirects to a DataNode (node2/node3:9864), which requests follows."""

    def __init__(self, url, timeout=30):
        u = urlparse(url)
        self.base = f"{u.scheme}://{u.netloc}/webhdfs/v1{u.path.rstrip('/')}"
        self.timeout = timeout
        self.http = requests.Session()

    def _get(self, path, op, stream=False):
        r = self.http.get(f"{self.base}/{path}".rstrip("/"), params={"op": op},
                          timeout=self.timeout, stream=stream)
        r.raise_for_status()
        return r

    def list(self, path=""):
        """[(name, is_dir, length)]; [] if the path does not exist."""
        try:
            statuses = self._get(path, "LISTSTATUS").json()["FileStatuses"]["FileStatus"]
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return []
            raise
        return [(s["pathSuffix"], s["type"] == "DIRECTORY", s["length"]) for s in statuses]

    def read_bytes(self, path):
        return self._get(path, "OPEN").content

    def download(self, path, dest):
        with self._get(path, "OPEN", stream=True) as r, open(dest, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)


class LocalSource:
    """Same interface over a local directory, for testing with make_fake_export.py."""

    def __init__(self, url):
        self.root = Path(urlparse(url).path)

    def list(self, path=""):
        p = self.root / path
        if not p.is_dir():
            return []
        return [(c.name, c.is_dir(), 0 if c.is_dir() else c.stat().st_size) for c in sorted(p.iterdir())]

    def read_bytes(self, path):
        return (self.root / path).read_bytes()

    def download(self, path, dest):
        shutil.copyfile(self.root / path, dest)


def make_source(url=EXPORT_SOURCE):
    return LocalSource(url) if url.startswith("file://") else WebHdfsSource(url)


def local_versions(serving=SERVING_DIR):
    p = Path(serving)
    if not p.is_dir():
        return []
    return sorted(int(m.group(1)) for c in p.iterdir() if c.is_dir() and (m := VERSION_DIR.match(c.name)))


def remote_complete_versions(source):
    """Versions whose directory has _SUCCESS. Anything without it is still being written, or failed."""
    versions = []
    for name, is_dir, _ in source.list():
        m = VERSION_DIR.match(name)
        if is_dir and m and any(n == "_SUCCESS" for n, _, _ in source.list(name)):
            versions.append(int(m.group(1)))
    return sorted(versions)


def _same_export(a, b):
    return (a.get("created_at"), a.get("documents_snapshot_id")) == (b.get("created_at"), b.get("documents_snapshot_id"))


def pull_version(source, n, serving=SERVING_DIR):
    """Download, validate and atomically publish vN. Raises on any failure (vN.tmp is cleaned up)."""
    serving = Path(serving)
    final, tmp = serving / f"v{n}", serving / f"v{n}.tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    try:
        manifest_bytes = source.read_bytes(f"v{n}/manifest.json")
        manifest = json.loads(manifest_bytes)
        if int(manifest.get("version", -1)) != n:
            raise InvalidExport(f"manifest.json in v{n} says version {manifest.get('version')}")
        (tmp / "manifest.json").write_bytes(manifest_bytes)
        for table in TABLES:
            (tmp / table).mkdir()
            files = [(name, size) for name, is_dir, size in source.list(f"v{n}/{table}")
                     if not is_dir and name.endswith(".parquet")]
            if not files:
                raise InvalidExport(f"v{n}/{table}/ has no parquet files on the source")
            for name, size in files:
                dest = tmp / table / name
                source.download(f"v{n}/{table}/{name}", dest)
                if dest.stat().st_size != size:
                    raise InvalidExport(f"{table}/{name}: got {dest.stat().st_size} bytes, expected {size}")
        read_export(tmp)
        # An export --force could have replaced vN while we were downloading; then retry next poll.
        if not _same_export(manifest, json.loads(source.read_bytes(f"v{n}/manifest.json"))):
            raise InvalidExport(f"v{n} was re-exported during download")
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise

    if final.exists():  # re-export of the same version: swap the directory, then drop the old one
        old = serving / f"v{n}.old"
        shutil.rmtree(old, ignore_errors=True)
        final.rename(old)
        tmp.rename(final)
        shutil.rmtree(old, ignore_errors=True)
    else:
        tmp.rename(final)
    log.info("pulled v%d: %d docs, %d postings", n, manifest["num_docs"], manifest["num_postings"])
    return manifest


class Puller:
    """Remembers failed (version, created_at) pairs so a broken export is not re-downloaded every poll."""

    def __init__(self, source=None, serving=SERVING_DIR):
        self.source = source or make_source()
        self.serving = serving
        self.failed = {}  # version -> (created_at, error)

    def sync(self, only=None):
        """Pull every complete remote version that is missing or outdated locally. Returns pulled versions."""
        Path(self.serving).mkdir(parents=True, exist_ok=True)
        pulled = []
        targets = [only] if only is not None else remote_complete_versions(self.source)
        for n in targets:
            try:
                remote = json.loads(self.source.read_bytes(f"v{n}/manifest.json"))
            except Exception as e:
                log.warning("v%d: cannot read remote manifest: %s", n, e)
                continue
            try:
                if _same_export(remote, read_manifest(Path(self.serving) / f"v{n}")):
                    continue
            except InvalidExport:
                pass  # not on disk yet
            if only is None and self.failed.get(n, (None,))[0] == remote.get("created_at"):
                continue
            try:
                pull_version(self.source, n, self.serving)
                self.failed.pop(n, None)
                pulled.append(n)
            except Exception as e:
                log.error("v%d NOT pulled, keeping current version: %s", n, e)
                self.failed[n] = (remote.get("created_at"), str(e))
        return pulled


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", default=EXPORT_SOURCE)
    ap.add_argument("--serving", default=SERVING_DIR)
    ap.add_argument("--version", type=int, help="pull just this version, then exit")
    ap.add_argument("--once", action="store_true", help="one check, then exit")
    ap.add_argument("--interval", type=float, default=10)
    ap.add_argument("--notify", default="http://localhost:8000/reload",
                    help="POST here after a pull so the API loads it ('' to disable)")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    puller = Puller(make_source(a.source), a.serving)
    while True:
        try:
            pulled = puller.sync(a.version)
        except Exception as e:  # cluster unreachable: keep polling
            log.warning("source unreachable: %s", e)
            pulled = []
        if pulled and a.notify:
            try:
                requests.post(a.notify, timeout=60)
            except requests.RequestException as e:
                log.warning("could not notify API at %s: %s", a.notify, e)
        if a.version is not None:
            raise SystemExit(0 if pulled or a.version in local_versions(a.serving) else 1)
        if a.once:
            return
        time.sleep(a.interval)


if __name__ == "__main__":
    main()
