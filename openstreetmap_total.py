#!/usr/bin/env python3
"""
Inventory an OpenStreetMap .osm.pbf file, with a live progress bar.

Streams the file once and reports:
  * entity counts by type (node / way / relation)
  * every metadata attribute present on each type (id, version, timestamp, ...)
  * every tag key per type, with how many entities use it
  * every (key, value) pair per type (optional, memory-heavy on planet files)
  * for ways: how many node refs they have; for relations: member types and roles
  * key combinations ("amenity+name+opening_hours") that appear together

Progress is shown as the percentage of the file's bytes that have been parsed.
The progress bar and summary go to the terminal; the report goes to stdout.

Usage:
    python3 osm_inventory.py planet-260928.osm.pbf
    python3 osm_inventory.py planet-260928.osm.pbf --values --json report.json
    python3 osm_inventory.py planet-260928.osm.pbf --values --top 50

Requires:  pip install osmium
"""

import argparse
import json
import multiprocessing
import os
import queue
import sys
import tempfile
import threading
import time
from collections import Counter, defaultdict

import osmium


ENTITY_TYPES = ("node", "way", "relation")
CHUNK = 8 * 1024 * 1024  # bytes copied into the pipe per read


class Inventory(osmium.SimpleHandler):
    def __init__(self, track_values: bool, max_combo_keys: int):
        super().__init__()
        self.track_values = track_values
        self.max_combo_keys = max_combo_keys

        self.counts = Counter()                       # entity type -> count
        self.tagged_counts = Counter()                # entity type -> entities with >=1 tag
        self.attr_presence = defaultdict(Counter)     # type -> attr -> count
        self.key_counts = defaultdict(Counter)        # type -> key -> count
        self.value_counts = defaultdict(Counter)      # type -> (key, value) -> count
        self.combo_counts = defaultdict(Counter)      # type -> "k1+k2+..." -> count
        self.member_types = Counter()                 # relation member type -> count
        self.member_roles = Counter()                 # (member type, role) -> count
        self.way_node_hist = Counter()                # bucketed node-count -> ways
        self.relation_member_hist = Counter()         # bucketed member-count -> relations

    # ---- helpers -------------------------------------------------------
    def _tags(self, etype, obj):
        tags = list(obj.tags)
        self.counts[etype] += 1
        # Record only attributes that are actually set on this object.
        ap = self.attr_presence[etype]
        ap["id"] += 1
        if obj.version:
            ap["version"] += 1
        if obj.changeset:
            ap["changeset"] += 1
        if obj.uid is not None and obj.uid >= 0:
            ap["uid"] += 1
        if obj.user:
            ap["user"] += 1
        if obj.timestamp is not None and obj.timestamp.year > 1970:
            ap["timestamp"] += 1
        if obj.visible:
            ap["visible"] += 1

        if not tags:
            return
        self.tagged_counts[etype] += 1
        keys = []
        for t in tags:
            self.key_counts[etype][t.k] += 1
            keys.append(t.k)
            if self.track_values:
                self.value_counts[etype][(t.k, t.v)] += 1
        if self.max_combo_keys:
            combo = "+".join(sorted(keys)[: self.max_combo_keys])
            self.combo_counts[etype][combo] += 1

    # ---- osmium callbacks ---------------------------------------------
    def node(self, n):
        self._tags("node", n)
        if n.location.valid():
            self.attr_presence["node"]["lat/lon"] += 1

    def way(self, w):
        self._tags("way", w)
        self.way_node_hist[_bucket(len(w.nodes))] += 1

    def relation(self, r):
        self._tags("relation", r)
        members = list(r.members)
        self.relation_member_hist[_bucket(len(members))] += 1
        for m in members:
            self.member_types[m.type] += 1
            self.member_roles[(m.type, m.role or "")] += 1


def _bucket(n: int) -> str:
    """Log-ish buckets so the histogram stays readable."""
    for edge in (0, 1, 2, 3, 4, 5, 10, 20, 50, 100, 500, 1000, 10000):
        if n <= edge:
            return f"<={edge}"
    return ">10000"


# ---- progress display ----------------------------------------------------
def _fmt_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} TB"


def _fmt_time(seconds: float) -> str:
    s = int(seconds)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    return f"{h}h{m:02d}m{s:02d}s" if h else f"{m}m{s:02d}s"


def _draw_progress(done: int, total: int, elapsed: float):
    pct = 100.0 * done / total if total else 0.0
    rate = done / elapsed if elapsed > 0 else 0.0
    eta = _fmt_time((total - done) / rate) if rate > 0 and done < total else "--"
    width = 30
    filled = min(width, int(width * pct / 100))
    bar = "#" * filled + "-" * (width - filled)
    sys.stderr.write(
        f"\r[{bar}] {pct:5.1f}%  {_fmt_bytes(done)} / {_fmt_bytes(total)}  "
        f"{_fmt_bytes(rate)}/s  elapsed {_fmt_time(elapsed)}  ETA {eta}   "
    )
    sys.stderr.flush()


# ---- scanning ----------------------------------------------------------------
class _Feeder(threading.Thread):
    """Copies the PBF file into a named pipe. The scan worker reads the pipe, so
    the bytes copied so far track how much of the file has been parsed."""

    def __init__(self, src: str, fifo: str):
        super().__init__(daemon=True)
        self.src = src
        self.fifo = fifo
        self.bytes_done = 0
        self.error = None

    def run(self):
        try:
            with open(self.src, "rb") as fin, open(self.fifo, "wb") as fout:
                while True:
                    buf = fin.read(CHUNK)
                    if not buf:
                        break
                    fout.write(buf)
                    self.bytes_done += len(buf)
        except Exception as e:
            self.error = e


def _scan_worker(fifo, track_values, max_combo_keys, result_q):
    """Runs in a separate process. pyosmium holds the GIL while it parses, so the
    scan must not share a process with the progress display."""
    inv = Inventory(track_values=track_values, max_combo_keys=max_combo_keys)
    start = time.time()
    try:
        # locations=False: we don't need node coordinates for attribute analysis
        inv.apply_file(fifo, locations=False)
        result_q.put(("ok", time.time() - start, dict(inv.__dict__)))
    except Exception as e:
        result_q.put(("err", str(e), None))


def scan_with_progress(path: str, track_values: bool, max_combo_keys: int) -> Inventory:
    total = os.path.getsize(path)
    tmpdir = tempfile.mkdtemp(prefix="osm_inv_")
    # Keep the original file name so libosmium detects the format from the extension.
    fifo = os.path.join(tmpdir, os.path.basename(path))
    os.mkfifo(fifo)

    result_q = multiprocessing.Queue()
    worker = multiprocessing.Process(
        target=_scan_worker,
        args=(fifo, track_values, max_combo_keys, result_q),
        daemon=True,
    )
    feeder = _Feeder(path, fifo)

    start = time.time()
    worker.start()
    feeder.start()

    result = None
    try:
        while result is None:
            try:
                result = result_q.get(timeout=2.0)
            except queue.Empty:
                _draw_progress(feeder.bytes_done, total, time.time() - start)
                if not worker.is_alive():
                    # Give the queue one more chance to deliver a final message.
                    try:
                        result = result_q.get(timeout=2.0)
                    except queue.Empty:
                        raise RuntimeError("scan process exited without a result")
        _draw_progress(total, total, time.time() - start)
        sys.stderr.write("\n")
    finally:
        worker.join(timeout=5)
        if worker.is_alive():
            worker.terminate()
        try:
            os.unlink(fifo)
            os.rmdir(tmpdir)
        except OSError:
            pass

    status, payload, state = result
    if status == "err":
        raise RuntimeError(payload)
    if feeder.error is not None:
        raise feeder.error

    inv = Inventory.__new__(Inventory)
    inv.__dict__.update(state)
    inv.elapsed = payload
    return inv


# ---- reporting ---------------------------------------------------------
def print_report(inv: Inventory, top: int):
    out = sys.stdout.write
    line = "=" * 72

    out(f"\n{line}\nENTITY COUNTS  (scan took {inv.elapsed:,.1f}s)\n{line}\n")
    for t in ENTITY_TYPES:
        out(f"  {t:<10} {inv.counts[t]:>16,}   tagged: {inv.tagged_counts[t]:>14,}\n")

    out(f"\n{line}\nMETADATA ATTRIBUTES PRESENT PER TYPE\n{line}\n")
    for t in ENTITY_TYPES:
        if not inv.counts[t]:
            continue
        out(f"  {t}:\n")
        for attr, c in sorted(inv.attr_presence[t].items()):
            out(f"    {attr:<12} {c:>16,}\n")

    out(f"\n{line}\nTAG KEYS PER TYPE (top {top})\n{line}\n")
    for t in ENTITY_TYPES:
        keys = inv.key_counts[t]
        if not keys:
            continue
        out(f"  {t}: {len(keys):,} distinct keys\n")
        for k, c in keys.most_common(top):
            out(f"    {k:<40} {c:>14,}\n")

    if inv.value_counts:
        out(f"\n{line}\nTAG KEY=VALUE PAIRS PER TYPE (top {top} by count)\n{line}\n")
        for t in ENTITY_TYPES:
            pairs = inv.value_counts[t]
            if not pairs:
                continue
            out(f"  {t}: {len(pairs):,} distinct key=value pairs\n")
            for (k, v), c in pairs.most_common(top):
                out(f"    {k}={v[:60]:<60} {c:>12,}\n")

    if inv.combo_counts:
        out(f"\n{line}\nMOST COMMON KEY COMBINATIONS PER TYPE (top {top})\n{line}\n")
        for t in ENTITY_TYPES:
            combos = inv.combo_counts[t]
            if not combos:
                continue
            out(f"  {t}: {len(combos):,} distinct key combinations\n")
            for combo, c in combos.most_common(top):
                out(f"    {combo[:90]:<90} {c:>10,}\n")

    out(f"\n{line}\nWAY NODE-COUNT HISTOGRAM\n{line}\n")
    for b, c in sorted(inv.way_node_hist.items(), key=lambda x: _sort_key(x[0])):
        out(f"  {b:<10} {c:>14,}\n")

    out(f"\n{line}\nRELATION MEMBER-COUNT HISTOGRAM\n{line}\n")
    for b, c in sorted(inv.relation_member_hist.items(), key=lambda x: _sort_key(x[0])):
        out(f"  {b:<10} {c:>14,}\n")

    if inv.member_types:
        out(f"\n{line}\nRELATION MEMBER TYPES AND ROLES (top {top} roles)\n{line}\n")
        for mt, c in inv.member_types.most_common():
            out(f"  {mt:<10} {c:>14,}\n")
        out("  roles:\n")
        for (mt, role), c in inv.member_roles.most_common(top):
            label = role if role else "(empty)"
            out(f"    {mt:<10} {label:<30} {c:>12,}\n")
    out("\n")


def _sort_key(b: str):
    if b.startswith("<="):
        return int(b[2:])
    return 10**12


def to_json(inv: Inventory) -> dict:
    return {
        "counts": dict(inv.counts),
        "tagged_counts": dict(inv.tagged_counts),
        "attributes": {t: dict(inv.attr_presence[t]) for t in ENTITY_TYPES},
        "tag_keys": {t: dict(inv.key_counts[t]) for t in ENTITY_TYPES},
        "tag_key_value_pairs": {
            t: {f"{k}={v}": c for (k, v), c in inv.value_counts[t].items()}
            for t in ENTITY_TYPES
        } if inv.track_values else {},
        "key_combinations": {t: dict(inv.combo_counts[t]) for t in ENTITY_TYPES},
        "way_node_histogram": dict(inv.way_node_hist),
        "relation_member_histogram": dict(inv.relation_member_hist),
        "relation_member_types": dict(inv.member_types),
        "relation_member_roles": {f"{mt}|{role}": c for (mt, role), c in inv.member_roles.items()},
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("pbf", help="path to the .osm.pbf file")
    ap.add_argument("--values", action="store_true",
                    help="also count every key=value pair (uses a lot of RAM on planet-size files)")
    ap.add_argument("--combos", type=int, default=6, metavar="N",
                    help="track key combinations using up to N keys per entity (0 disables; default 6)")
    ap.add_argument("--top", type=int, default=40, help="rows to show per table (default 40)")
    ap.add_argument("--json", metavar="FILE", help="also write the full inventory as JSON")
    args = ap.parse_args()

    print(f"Scanning {args.pbf} ...", file=sys.stderr)
    try:
        inv = scan_with_progress(args.pbf, args.values, args.combos)
    except (RuntimeError, OSError) as e:
        sys.stderr.write("\n")
        print(f"Error reading file: {e}", file=sys.stderr)
        sys.exit(1)

    print_report(inv, args.top)

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(to_json(inv), f, ensure_ascii=False)
        print(f"Full inventory written to {args.json}", file=sys.stderr)


if __name__ == "__main__":
    main()