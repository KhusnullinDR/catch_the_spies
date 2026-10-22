import re
import time
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import pymupdf
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

SCHEMA = pa.schema([
    ("origin_code",      pa.string()),
    ("origin_city",      pa.string()),
    ("origin_country",   pa.string()),
    ("dest_code",        pa.string()),
    ("dest_city",        pa.string()),
    ("dest_country",     pa.string()),
    ("airline_code",     pa.string()),
    ("flight_number",    pa.string()),
    ("valid_from",       pa.date32()),
    ("valid_to",         pa.date32()),
    ("days_mask",        pa.string()),
    ("dep_time",         pa.string()),
    ("arr_time",         pa.string()),
    ("arr_day_offset",   pa.int8()),
    ("aircraft_code",    pa.string()),
    ("travel_time_min",  pa.int32()),
    ("source_file",      pa.string()),
    ("source_page",      pa.int32()),
])

MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}

HEADER_TOKENS = {'Validity', 'Days', 'Dep', 'Time',
                 'Arr', 'Flight', 'Aircraft', 'Travel'}

VALIDITY_RE = re.compile(r'^(\d{1,2})\s+([A-Za-z]{3})\s*-\s*(\d{1,2})\s+([A-Za-z]{3})$')
DAYS_DEP_RE = re.compile(r'^([1-7](?:\s*[1-7])*)\s+(\d{1,2}:\d{2})$')   # \s* между днями
ARR_RE      = re.compile(r'^(\d{1,2}:\d{2})(\+\d)?$')
FLIGHT_RE   = re.compile(r'^([A-Z0-9]{2})\s?(\d{1,4})$')
AIRCRAFT_RE = re.compile(r'^[A-Z0-9]{2,3}$')
TRAVEL_RE   = re.compile(r'^(\d{1,2})H(\d{1,2})M?$')

FROM_RE = re.compile(r'^\s*FROM\s*:\s*(.*)$', re.I)
TO_RE   = re.compile(r'^\s*TO\s*:\s*(.*)$', re.I)
GEO_RE  = re.compile(r'^(.+?)\s*,\s*(.+)$')
CODE_RE = re.compile(r'^([A-Z]{3})$')


def normalize(line: str) -> str:
    return (line.replace('\u00a0', ' ')
                .replace('\u200b', '')
                .replace('\u2010', '-')
                .replace('\u2011', '-')
                .replace('\u2012', '-')
                .replace('\u2013', '-')
                .replace('\u2014', '-')
                .replace('\t', ' ')).strip()


def resolve_years(vf_day, vf_mon, vt_day, vt_mon, base_year, start_month=11):
    m_vf = MONTHS[vf_mon.title()]
    m_vt = MONTHS[vt_mon.title()]

    year_vf = base_year if m_vf >= start_month else base_year + 1
    year_vt = year_vf if m_vt >= m_vf else year_vf + 1

    vf = date(year_vf, m_vf, int(vf_day))
    vt = date(year_vt, m_vt, int(vt_day))
    return vf, vt


def parse_geo(text: str):
    m = GEO_RE.match(text)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return text.strip(), ''


def parse_travel_time(s: str):
    m = TRAVEL_RE.match(s.strip().upper())
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    return None

def process_range(task):
    (pdf_path, out_path, base_year, start_month,
     read_start, read_end, emit_start, emit_end, batch_rows) = task

    pdf_name = Path(pdf_path).name
    writer = pq.ParquetWriter(out_path, SCHEMA, compression='snappy')
    batch = {f.name: [] for f in SCHEMA}
    pending = []
    from_state = {'code': None, 'city': None, 'country': None}
    to_state   = {'code': None, 'city': None, 'country': None}
    phase = 'idle'
    emitted = 0
    scanned = 0

    def flush():
        if not batch['origin_code']:
            return
        writer.write_table(pa.table(batch, schema=SCHEMA))
        for k in batch:
            batch[k].clear()

    def add_record(page_num: int):
        nonlocal pending, emitted
        p = pending
        pending = []
        if len(p) != 6:
            return
        v = VALIDITY_RE.match(p[0])
        d = DAYS_DEP_RE.match(p[1])
        a = ARR_RE.match(p[2])
        f = FLIGHT_RE.match(p[3])
        ac = AIRCRAFT_RE.match(p[4])
        t = TRAVEL_RE.match(p[5])
        if not (v and d and a and f and ac and t):
            return
        if not (from_state['code'] and to_state['code']):
            return
        if not (emit_start <= page_num <= emit_end):
            return

        vf, vt = resolve_years(v.group(1), v.group(2),
                               v.group(3), v.group(4),
                               base_year, start_month)
        days = ''.join(d.group(1).split())
        offset = int(a.group(2)[1:]) if a.group(2) else 0
        fl = f.group(1) + f.group(2)

        batch['origin_code'].append(from_state['code'])
        batch['origin_city'].append(from_state['city'])
        batch['origin_country'].append(from_state['country'])
        batch['dest_code'].append(to_state['code'])
        batch['dest_city'].append(to_state['city'])
        batch['dest_country'].append(to_state['country'])
        batch['airline_code'].append(f.group(1))
        batch['flight_number'].append(fl)
        batch['valid_from'].append(vf)
        batch['valid_to'].append(vt)
        batch['days_mask'].append(days)
        batch['dep_time'].append(d.group(2))
        batch['arr_time'].append(a.group(1))
        batch['arr_day_offset'].append(offset)
        batch['aircraft_code'].append(ac.group(0))
        batch['travel_time_min'].append(parse_travel_time(t.group(0)))
        batch['source_file'].append(pdf_name)
        batch['source_page'].append(page_num)
        emitted += 1
        if emitted % batch_rows == 0:
            flush()

    doc = pymupdf.open(pdf_path)
    try:
        for page_num in range(read_start, read_end + 1):
            page = doc[page_num - 1]
            try:
                tp = page.get_textpage()
                text = tp.extractText()
                tp = None
            except Exception:
                text = page.get_text('text')

            for raw in text.splitlines():
                scanned += 1
                line = normalize(raw)
                if not line:
                    continue
                if line in HEADER_TOKENS:
                    continue

                m = FROM_RE.match(line)
                if m:
                    if pending:
                        add_record(page_num)
                    rest = m.group(1).strip()
                    if rest:
                        from_state['city'], from_state['country'] = parse_geo(rest)
                        phase = 'from_code'
                    else:
                        phase = 'from_city'
                    continue

                m = TO_RE.match(line)
                if m:
                    if pending:
                        add_record(page_num)
                    rest = m.group(1).strip()
                    if rest:
                        to_state['city'], to_state['country'] = parse_geo(rest)
                        phase = 'to_code'
                    else:
                        phase = 'to_city'
                    continue

                if phase == 'from_city':
                    from_state['city'], from_state['country'] = parse_geo(line)
                    phase = 'from_code'
                    continue

                if phase == 'from_code':
                    m2 = CODE_RE.match(line)
                    if m2:
                        from_state['code'] = m2.group(1)
                    phase = 'idle'
                    continue

                if phase == 'to_city':
                    to_state['city'], to_state['country'] = parse_geo(line)
                    phase = 'to_code'
                    continue

                if phase == 'to_code':
                    m2 = CODE_RE.match(line)
                    if m2:
                        to_state['code'] = m2.group(1)
                    phase = 'idle'
                    continue

                if not (from_state['code'] and to_state['code']):
                    continue

                if line.startswith('+') and pending:
                    pending[-1] += line
                    continue

                pending.append(line)
                if len(pending) == 6:
                    add_record(page_num)

            if page_num % 500 == 0:
                flush()
    finally:
        flush()
        writer.close()
        doc.close()

    return {'part': str(out_path), 'emitted': emitted, 'scanned': scanned}

def parallel_parse(pdf_path, out_dir, base_year,
                   start_month=11,
                   n_workers=None, overlap=20,
                   page_start=None, page_end=None,
                   batch_rows=100_000):
    pdf_path = Path(pdf_path)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    parts_dir = out_dir / 'parts'
    parts_dir.mkdir(exist_ok=True)

    doc = pymupdf.open(pdf_path)
    total_pages = doc.page_count
    doc.close()

    ps = page_start or 1
    pe = page_end or total_pages
    if ps > pe:
        raise ValueError(f"page_start ({ps}) > page_end ({pe})")
    effective = pe - ps + 1

    n_workers = n_workers or max(1, mp.cpu_count() - 1)
    step = (effective + n_workers - 1) // n_workers

    tasks = []
    for i in range(n_workers):
        emit_start = ps + i * step
        emit_end = min(ps + (i + 1) * step - 1, pe)
        if emit_start > pe:
            break
        read_start = max(ps, emit_start - overlap)
        read_end = emit_end
        part_path = parts_dir / f'part_{i:03d}.parquet'
        tasks.append((str(pdf_path), str(part_path), base_year, start_month,
                      read_start, read_end, emit_start, emit_end, batch_rows))

    print(f"workers: {len(tasks)}, pages: {effective}, "
          f"~{step} pages/worker, overlap={overlap}, "
          f"base_year={base_year}, start_month={start_month}")

    t0 = time.time()
    total_rows = 0
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        futures = {ex.submit(process_range, t): t for t in tasks}
        for fut in as_completed(futures):
            t = futures[fut]
            try:
                r = fut.result()
                total_rows += r['emitted']
                print(f"[{time.time()-t0:6.0f}s] done {r['part']} "
                      f"-> emitted={r['emitted']}, scanned_lines={r['scanned']}")
            except Exception as e:
                print(f"FAILED {t[1]}: {e}")
                raise

    print(f"all parts done: raw rows={total_rows}, time={time.time()-t0:.0f}s")

    parts_glob = str(parts_dir / 'part_*.parquet')
    final_path = out_dir / 'flights.parquet'
    print(f"merging + dedup -> {final_path} ...")
    t1 = time.time()

    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT DISTINCT ON (
                origin_code, dest_code, flight_number,
                valid_from, valid_to, days_mask,
                dep_time, arr_time, aircraft_code
            ) *
            FROM read_parquet('{parts_glob}')
            ORDER BY origin_code, dest_code, flight_number,
                     valid_from, valid_to, days_mask,
                     dep_time, arr_time, aircraft_code, source_page
        ) TO '{final_path}' (FORMAT PARQUET, COMPRESSION 'snappy')
    """)
    n_final = con.execute(f"SELECT COUNT(*) FROM '{final_path}'").fetchone()[0]
    con.close()

    print(f"final: {n_final} unique rows, time={time.time()-t1:.0f}s")
    print(f"total time: {time.time()-t0:.0f}s")
    return n_final


if __name__ == '__main__':
    base = Path(__file__).resolve().parent
    
    parallel_parse(
        pdf_path=base / 'data' / 'Skyteam_Timetable.pdf',
        out_dir=base / 'out',
        base_year=2018,
        start_month=11,
        n_workers=max(1, mp.cpu_count() - 1),
        overlap=20,
        batch_rows=100_000,
    )