#!/usr/bin/env python3
"""
Polls Calgary Transit's GTFS-realtime feeds and uploads them to a Databricks Volume.

Each entity becomes one JSON line. Lines are buffered and written out every few minutes as one
gzipped file per feed, so Databricks isn't flooded with tiny files. Files sit in data/spool until
they upload successfully.

    python poller.py                 # poll and upload
    python poller.py --no-upload     # poll only, upload later with --upload-only
    python poller.py --upload-only   # upload whatever is in the spool and exit
    python poller.py --static        # download the schedule zip and upload it
    python poller.py --check         # check credentials and that the Volume exists
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

# transitdata.calgary.ca/ctransit/*.pb (what most GTFS directories list) times out now,
# so these are the open data portal downloads instead.
PORTAL = "https://data.calgary.ca/download/{dataset}/application%2Foctet-stream"
FEEDS = {
    "vehicle_positions": {"url": PORTAL.format(dataset="am7c-qe3u"), "interval_s": 30},
    "trip_updates": {"url": PORTAL.format(dataset="gs4m-mdc2"), "interval_s": 30},
    "alerts": {"url": PORTAL.format(dataset="jhgn-ynqj"), "interval_s": 300},
}
STATIC_GTFS_URL = "https://data.calgary.ca/download/npk7-z3bj/application%2Fzip"

DEFAULT_VOLUME = "/Volumes/workspace/transit_bronze/landing"
ROOT = Path(__file__).resolve().parent.parent
SPOOL_DIR = ROOT / "data" / "spool"
STATE_FILE = ROOT / "data" / "poller_state.json"
STATIC_STATE_FILE = ROOT / "data" / "static_state.json"  # kept separate, the poller overwrites STATE_FILE

log = logging.getLogger("poller")
_running = True


def _stop(signum, _frame):
    global _running
    log.info("Received signal %s, flushing and shutting down...", signum)
    _running = False


def fetch(session: requests.Session, url: str, retries: int = 3) -> bytes | None:
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as exc:
            wait = 2 ** attempt
            log.warning("Fetch failed (%s), attempt %d/%d, retrying in %ss",
                        exc, attempt + 1, retries, wait)
            time.sleep(wait)
    return None


def decode(raw: bytes) -> dict:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(raw)
    return MessageToDict(msg, preserving_proto_field_name=True)


def fetch_and_decode(session: requests.Session, name: str, url: str, attempts: int = 4) -> dict | None:
    # The portal sometimes serves the file mid-rewrite and it won't parse. Retry a few times.
    for attempt in range(1, attempts + 1):
        raw = fetch(session, url)
        if raw is None:
            return None
        try:
            return decode(raw)
        except Exception:
            if attempt < attempts:
                time.sleep(3)
    log.warning("Skipped %s poll: feed still unreadable after %d downloads", name, attempts)
    return None


def to_lines(feed_name: str, feed: dict, poll_ts: datetime) -> list[str]:
    header = feed.get("header", {})
    feed_ts = int(header.get("timestamp", 0) or 0)
    envelope = {
        "feed": feed_name,
        "feed_ts": feed_ts,
        "poll_ts": poll_ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "gtfs_rt_version": header.get("gtfs_realtime_version"),
    }
    return [
        json.dumps({**envelope, "entity_id": e.get("id"), "entity_json": json.dumps(e)})
        for e in feed.get("entity", [])
    ]


def write_spool_file(feed_name: str, lines: list[str], now: datetime) -> Path:
    folder = SPOOL_DIR / feed_name / f"date={now:%Y-%m-%d}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{feed_name}_{now:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:8]}.json.gz"
    tmp = path.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    tmp.rename(path)
    return path


class Uploader:
    # Plain REST calls instead of databricks-sdk. The SDK pulled in google-auth/cryptography,
    # which wouldn't import on my Mac's Python 3.9.

    def __init__(self, volume: str):
        host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        token = os.environ.get("DATABRICKS_TOKEN", "")
        if not host or not token:
            sys.exit("Set DATABRICKS_HOST and DATABRICKS_TOKEN (see .env.example), or use --no-upload")
        self.host = host
        self.volume = volume.rstrip("/")
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {token}"

    def _url(self, kind: str, path: str) -> str:
        return f"{self.host}/api/2.0/fs/{kind}{quote(path)}"

    def check(self) -> None:
        me = self.session.get(f"{self.host}/api/2.0/preview/scim/v2/Me", timeout=30)
        me.raise_for_status()
        print("Connected as", me.json().get("userName"))
        vol = self.session.head(self._url("directories", self.volume), timeout=30)
        if vol.status_code == 200:
            print("Volume found:", self.volume)
        else:
            print(f"Volume NOT found ({vol.status_code}): {self.volume} -> run notebooks/00_setup.sql first")

    def upload(self, local: Path, remote_rel: str) -> None:
        resp = self.session.put(
            self._url("files", f"{self.volume}/{remote_rel}"),
            params={"overwrite": "true"},
            data=local.read_bytes(),
            headers={"Content-Type": "application/octet-stream"},
            timeout=(15, 120),
        )
        if resp.status_code >= 300:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")

    def drain_spool(self) -> int:
        sent = 0
        for path in sorted(SPOOL_DIR.rglob("*.json.gz")):
            rel = path.relative_to(SPOOL_DIR).as_posix()
            try:
                self.upload(path, rel)
                path.unlink()
                sent += 1
            except Exception as exc:  # leave it in the spool, try again next flush
                log.error("Upload failed for %s: %s", rel, exc)
                break
        return sent


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def upload_static(uploader: Uploader | None) -> None:
    log.info("Downloading static GTFS from %s", STATIC_GTFS_URL)
    resp = requests.get(STATIC_GTFS_URL, timeout=300)
    resp.raise_for_status()
    digest = hashlib.sha256(resp.content).hexdigest()[:10]
    state = json.loads(STATIC_STATE_FILE.read_text()) if STATIC_STATE_FILE.exists() else {}
    if state.get("static_hash") == digest:
        log.info("Static GTFS unchanged (hash %s), nothing to do.", digest)
        return
    now = datetime.now(timezone.utc)
    name = f"gtfs_{now:%Y%m%d}_{digest}.zip"
    local = ROOT / "data" / "static" / name
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(resp.content)
    log.info("Saved %s (%.1f MB)", local, len(resp.content) / 1e6)
    if uploader:
        log.info("Uploading...")
        uploader.upload(local, f"gtfs_static/{name}")
        log.info("Uploaded to %s/gtfs_static/%s", uploader.volume, name)
        state["static_hash"] = digest
        STATIC_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATIC_STATE_FILE.write_text(json.dumps(state, indent=2))


def run(uploader: Uploader | None, flush_s: int) -> None:
    session = requests.Session()
    session.headers["User-Agent"] = "calgary-transit-lakehouse"
    state = load_state()
    last_ts = state.setdefault("last_feed_ts", {})
    next_poll = {name: 0.0 for name in FEEDS}
    buffers: dict[str, list[str]] = {name: [] for name in FEEDS}
    next_flush = time.time() + flush_s
    stats = {name: 0 for name in FEEDS}

    def flush():
        now = datetime.now(timezone.utc)
        for name, lines in buffers.items():
            if lines:
                path = write_spool_file(name, lines, now)
                log.info("Spooled %d %s entities -> %s", len(lines), name, path.name)
                lines.clear()
        save_state(state)
        if uploader:
            sent = uploader.drain_spool()
            if sent:
                log.info("Uploaded %d file(s) to Databricks", sent)

    while _running:
        now = time.time()
        for name, cfg in FEEDS.items():
            if now < next_poll[name]:
                continue
            next_poll[name] = now + cfg["interval_s"]
            feed = fetch_and_decode(session, name, cfg["url"])
            if feed is None:
                continue
            feed_ts = int(feed.get("header", {}).get("timestamp", 0) or 0)
            if feed_ts and feed_ts == last_ts.get(name):
                continue  # same feed as last time
            last_ts[name] = feed_ts
            lines = to_lines(name, feed, datetime.now(timezone.utc))
            buffers[name].extend(lines)
            stats[name] += len(lines)

        if time.time() >= next_flush:
            flush()
            log.info("Totals since start: %s", stats)
            next_flush = time.time() + flush_s
        time.sleep(1)

    flush()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--no-upload", action="store_true", help="collect locally only")
    parser.add_argument("--upload-only", action="store_true", help="upload spooled files and exit")
    parser.add_argument("--static", action="store_true", help="download + upload static GTFS and exit")
    parser.add_argument("--check", action="store_true", help="test Databricks credentials + Volume and exit")
    parser.add_argument("--flush-seconds", type=int, default=300)
    parser.add_argument("--volume", default=os.environ.get("TRANSIT_VOLUME", DEFAULT_VOLUME))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)])
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    uploader = None if args.no_upload else Uploader(args.volume)

    if args.check:
        if uploader is None:
            sys.exit("--check can't be combined with --no-upload")
        uploader.check()
        return
    if args.static:
        upload_static(uploader)
        return
    if args.upload_only:
        if uploader is None:
            sys.exit("--upload-only and --no-upload don't make sense together")
        log.info("Uploaded %d file(s)", uploader.drain_spool())
        return

    log.info("Polling %s | flush every %ss | upload=%s", list(FEEDS), args.flush_seconds, bool(uploader))
    run(uploader, args.flush_seconds)


if __name__ == "__main__":
    main()
