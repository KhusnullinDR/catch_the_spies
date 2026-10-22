#!/usr/bin/env python3

import argparse
import csv
import re

import pyarrow as pa
import pyarrow.parquet as pq

CYR = dict(zip("абвгдеёжзийклмнопрстуфхцчшщъыьэюя",
               ["a", "b", "v", "g", "d", "e", "e", "zh", "z", "i", "i", "k", "l", "m", "n", "o", "p", "r", "s", "t",
                "u", "f", "kh", "ts", "ch", "sh", "shch", "ie", "y", "", "e", "iu", "ia"]))


def translit(s):
    return "".join(CYR.get(ch, ch) for ch in s.lower()).upper()


def na(v):
    v = (v or "").strip()
    return None if v in ("", "N/A", "Not presented") else v


def sirena(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        next(f)
        for i, line in enumerate(f, start=2):
            g = lambda a, b: line[a:b].strip()
            name = g(0, 60)
            fl = g(120, 132)
            m = re.fullmatch(r"([A-Z0-9]{2}\d+)(NO|YES)", fl)
            fare = g(198, 216)
            add = g(216, 240)
            ff = re.fullmatch(r"FF#([A-Z0-9]{2}) (\d+)", g(240, 276))
            parts = name.split()
            rows.append({
                "line_no": i, "name_cyr": name, "name_lat": translit(name),
                "last_name": translit(parts[0]) if parts else None,
                "first_name": translit(parts[1]) if len(parts) > 1 else None,
                "middle_name": translit(" ".join(parts[2:])) if len(parts) > 2 else None,
                "birth_date": na(g(60, 72)), "dep_date": g(72, 84), "dep_time": g(84, 96),
                "arr_date": g(96, 108), "arr_time": g(108, 120),
                "flight": m[1] if m else fl, "codeshare": m[2] if m else None,
                "origin": g(132, 138), "dest": g(138, 144), "pnr": na(g(144, 150)), "e_ticket": na(g(150, 166)),
                "passport": na(g(168, 180)), "seat": na(g(180, 186)), "meal": na(g(186, 192)),
                "cabin_class": na(g(192, 198)), "fare": fare[:6] or None, "baggage": fare[6:] or None,
                "additional_info": na(re.sub(r"[A-Z0-9#]$", "", add)) if len(add) > 1 else None,
                "ff_airline": ff[1] if ff else None, "ff_number": ff[2] if ff else None,
                "agent": na(g(276, 336)),
            })
    return rows


def boarding(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        for i, r in enumerate(csv.DictReader(f, delimiter=";"), start=2):
            bd = na(r["PassengerBirthDate"])
            if bd:
                mm, dd, yy = bd.split("/")
                bd = f"{yy}-{mm}-{dd}"
            mid = na(r["PassengerSecondName"])
            rows.append({
                "line_no": i, "first_name": r["PassengerFirstName"].strip(), "middle_name": mid,
                "last_name": r["PassengerLastName"].strip(), "sex": r["PassengerSex"], "birth_date": bd,
                "passport": na(r["PassengerDocument"]), "pnr": na(r["BookingCode"]), "e_ticket": na(r["TicketNumber"]),
                "baggage_status": na(r["Baggage"]), "flight_date": r["FlightDate"], "flight_time": r["FlightTime"],
                "flight": r["FlightNumber"], "codeshare": r["CodeShare"], "destination_city": r["Destination"],
            })
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="Airlines")
    ap.add_argument("--out-dir", default="work/src")
    a = ap.parse_args()
    for name, fn, file in [("src_sirena", sirena, "Sirena-export-fixed.tab"),
                           ("src_boarding", boarding, "BoardingData.csv")]:
        rows = fn(f"{a.src}/{file}")
        pq.write_table(pa.Table.from_pylist(rows), f"{a.out_dir}/{name}.parquet", compression="zstd")
        print(name, len(rows))


if __name__ == "__main__":
    main()
