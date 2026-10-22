import re
import time
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

SCHEMA = pa.schema([
    ("flight_date",    pa.date32()),
    ("flight_number",  pa.string()),
    ("airline_code",   pa.string()),
    ("origin",         pa.string()),
    ("destination",    pa.string()),
    ("status",         pa.string()),
    ("ff_airline",     pa.string()),
    ("ff_number",      pa.string()),
    ("booking_class",  pa.string()),
    ("fare_code",      pa.string()),
    ("source_file",    pa.string()),
])

# regex
DATE_RE     = re.compile(r"^\s*'(\d{4}-\d{2}-\d{2})'\s*:\s*$")
FLIGHT_RE   = re.compile(r"^\s+([A-Z0-9]{2}\d{1,4})\s*:\s*$")
FIELD_RE    = re.compile(r"^\s+(FROM|TO|STATUS)\s*:\s*(\S.*?)\s*$")
FF_ENTRY_RE = re.compile(
    r"^\s+([A-Z0-9]{2})\s+(\d+)\s*:\s*\{\s*CLASS\s*:\s*([A-Z])\s*,\s*FARE\s*:\s*([A-Z0-9]+)\s*\}"
)


def _new_batch():
    return {f.name: [] for f in SCHEMA}


def parse_file(input_path, output_path, batch_rows=100_000,
               report_every=10.0, verbose=True):
    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    source_file = input_path.name

    writer = pq.ParquetWriter(output_path, SCHEMA, compression="snappy")
    batch = _new_batch()

    stats = {"flights": 0, "passengers": 0, "skipped": 0, "lines": 0}
    cur_date = None
    cur_flight = None
    cur_ffs = []

    def flush():
        if not batch["flight_date"]:
            return
        writer.write_table(pa.table(batch, schema=SCHEMA))
        for k in batch:
            batch[k].clear()

    def emit():
        nonlocal cur_flight, cur_ffs
        if cur_flight is None or cur_date is None:
            cur_flight = None
            cur_ffs = []
            return

        fnum = cur_flight["flight_number"]
        base = dict(
            flight_date=cur_date,
            flight_number=fnum,
            airline_code=fnum[:2],
            origin=cur_flight.get("origin"),
            destination=cur_flight.get("destination"),
            status=cur_flight.get("status"),
            source_file=source_file,
        )
        if cur_ffs:
            for ff in cur_ffs:
                row = {**base, **ff}
                for k in batch:
                    batch[k].append(row[k])
            stats["passengers"] += len(cur_ffs)
        else:
            row = {**base, "ff_airline": None, "ff_number": None,
                   "booking_class": None, "fare_code": None}
            for k in batch:
                batch[k].append(row[k])
        stats["flights"] += 1
        if len(batch["flight_date"]) >= batch_rows:
            flush()
        cur_flight = None
        cur_ffs = []

    t0 = time.time()
    last = t0

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            stats["lines"] += 1
            line = raw.rstrip("\r\n")
            if not line or not line.strip():
                continue

            lstripped = line.lstrip()

            if lstripped.startswith("'"):
                m = DATE_RE.match(line)
                if m:
                    emit()
                    y, mo, d = m.group(1).split("-")
                    cur_date = date(int(y), int(mo), int(d))
                continue

            if line.strip() == "FF:":
                continue

            m = FLIGHT_RE.match(line)
            if m:
                emit()
                cur_flight = {"flight_number": m.group(1)}
                cur_ffs = []
                continue

            if cur_flight is None:
                continue

            m = FIELD_RE.match(line)
            if m:
                key, val = m.group(1), m.group(2).strip()
                if key == "FROM":
                    cur_flight["origin"] = val.upper()[:3] if val else None
                elif key == "TO":
                    cur_flight["destination"] = val.upper()[:3] if val else None
                elif key == "STATUS":
                    cur_flight["status"] = val or None
                continue

            m = FF_ENTRY_RE.match(line)
            if m:
                cur_ffs.append(dict(
                    ff_airline=m.group(1),
                    ff_number=m.group(2),
                    booking_class=m.group(3),
                    fare_code=m.group(4),
                ))
                continue

            stats["skipped"] += 1

            now = time.time()
            if verbose and now - last >= report_every:
                last = now
                elapsed = now - t0
                print(f"  {source_file}: {stats['lines']:,} lines | "
                      f"{stats['lines']/elapsed:.0f} lines/s | "
                      f"flights={stats['flights']:,} | "
                      f"passengers={stats['passengers']:,}")

    emit()
    flush()
    writer.close()

    elapsed = time.time() - t0
    if verbose:
        rate = stats["lines"] / elapsed if elapsed > 0 else 0
        print(f"  done {source_file}: {stats['flights']:,} flights, "
              f"{stats['passengers']:,} passengers, {stats['skipped']:,} skipped, "
              f"{elapsed:.1f}s ({rate:.0f} lines/s)")
    return stats


def parse_directory(input_dir, output_path, n_workers=None, verbose=True):
    input_dir = Path(input_dir)
    files = sorted(list(input_dir.glob("*.txt")) + list(input_dir.glob("*.yaml")))
    if not files:
        raise FileNotFoundError(f"no .txt/.yaml files in {input_dir}")

    n_workers = n_workers or max(1, mp.cpu_count() - 1)
    parts_dir = Path(output_path).parent / "parts"
    parts_dir.mkdir(parents=True, exist_ok=True)

    print(f"parsing {len(files)} files with {n_workers} workers")

    def work(args):
        i, fp = args
        out = parts_dir / f"part_{i:04d}.parquet"
        return parse_file(fp, out, verbose=False)

    t0 = time.time()
    totals = {"flights": 0, "passengers": 0, "skipped": 0, "lines": 0}

    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        for i, st in enumerate(ex.map(work, enumerate(files))):
            for k in totals:
                totals[k] += st[k]
            step = max(1, len(files) // 20)
            if verbose and (i + 1) % step == 0:
                print(f"  {i+1}/{len(files)} files, "
                      f"flights={totals['flights']:,}, "
                      f"passengers={totals['passengers']:,}")

    print(f"merging -> {output_path}")
    glob = str(parts_dir / "part_*.parquet")
    con = duckdb.connect()
    con.execute(f"""
        COPY (SELECT * FROM read_parquet('{glob}'))
        TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'snappy')
    """)
    n = con.execute(f"SELECT COUNT(*) FROM '{output_path}'").fetchone()[0]
    con.close()

    print(f"done: {n:,} rows, {time.time()-t0:.1f}s, "
          f"flights={totals['flights']:,}, "
          f"passengers={totals['passengers']:,}")
    return n


def parse_any(input_path, output_path, n_workers=None, verbose=True):
    input_path = Path(input_path)
    if input_path.is_file():
        return parse_file(input_path, output_path, verbose=verbose)
    if input_path.is_dir():
        return parse_directory(input_path, output_path,
                               n_workers=n_workers, verbose=verbose)
    raise FileNotFoundError(f"not found: {input_path}")


if __name__ == "__main__":
    base = Path(__file__).resolve().parent

    parse_any(
        input_path=base / "data" / "SkyTeam-Exchange.yaml",
        output_path=base / "out" / "flight_events.parquet",
    )