#!/usr/bin/env python3

import argparse
import glob
import json
import os
from concurrent.futures import ProcessPoolExecutor

import openpyxl
import pyarrow as pa
import pyarrow.parquet as pq

FIELDS = {
    (1, 8): "sequence", (3, 1): "title", (3, 2): "full_name", (3, 6): "ff_card", (3, 8): "cabin_class",
    (5, 1): "flight", (5, 4): "from_city", (5, 8): "to_city",
    (7, 2): "gate", (7, 4): "from_code", (7, 8): "to_code",
    (9, 1): "date", (9, 3): "time", (9, 5): "operated_by",
    (11, 8): "seat", (13, 2): "pnr", (13, 5): "e_ticket",
}
LABELS = {(1, 1): "BOARDING PASS", (1, 6): "SEQUENCE:", (7, 1): "GATE", (7, 6): "->", (9, 2): "/",
          (11, 7): "SEAT", (13, 1): "PNR:", (13, 4): "E-TICKET"}
COLS = ["file", "sheet", "sheet_idx"] + list(FIELDS.values()) + ["anomalies"]


def parse_file(path):
    wb = openpyxl.load_workbook(path, read_only=True)
    out = []
    for idx, ws in enumerate(wb.worksheets, start=1):
        row = {"file": os.path.basename(path), "sheet": ws.title, "sheet_idx": idx}
        anomalies = []
        for r, cells in enumerate(ws.iter_rows(values_only=True), start=1):
            for c, v in enumerate(cells, start=1):
                if v is None:
                    continue
                if (r, c) in FIELDS:
                    row[FIELDS[(r, c)]] = str(v)
                elif (r, c) in LABELS:
                    if str(v) != LABELS[(r, c)]:
                        anomalies.append(f"label r{r}c{c}={v!r}")
                elif not (r == 11 and c == 1):
                    anomalies.append(f"r{r}c{c}={v!r}")
        row["anomalies"] = json.dumps(anomalies, ensure_ascii=False) if anomalies else None
        out.append(row)
    wb.close()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="Airlines/Tickets")
    ap.add_argument("--out", default="work/src/src_tickets.parquet")
    ap.add_argument("--workers", type=int, default=6)
    a = ap.parse_args()
    files = sorted(glob.glob(os.path.join(a.src, "*.xlsx")))
    schema = pa.schema([(c, pa.int32() if c == "sheet_idx" else pa.string()) for c in COLS])
    n = 0
    with ProcessPoolExecutor(a.workers) as ex, pq.ParquetWriter(a.out, schema, compression="zstd") as w:
        for rows in ex.map(parse_file, files):
            w.write_table(pa.Table.from_pylist(rows, schema=schema))
            n += len(rows)
    print(f"files: {len(files)}, boarding passes: {n}")


if __name__ == "__main__":
    main()
