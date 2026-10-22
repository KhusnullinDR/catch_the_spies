#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import ijson
import pyarrow as pa
import pyarrow.parquet as pq

BATCH = 200_000


class Sink:
    def __init__(self, path, schema):
        self.schema = schema
        self.writer = pq.ParquetWriter(path, schema, compression="zstd")
        self.rows = []
        self.count = 0

    def add(self, row):
        self.rows.append(row)
        if len(self.rows) >= BATCH:
            self.flush()

    def flush(self):
        if self.rows:
            self.writer.write_table(pa.Table.from_pylist(self.rows, schema=self.schema))
            self.count += len(self.rows)
            self.rows = []

    def close(self):
        self.flush()
        self.writer.close()
        return self.count


def s(*names):
    return pa.schema([(n, pa.string()) for n in names])


def extract_json(path, out):
    profiles = Sink(out / "src_json_profiles.parquet", pa.schema([
        ("nickname", pa.string()), ("profile_idx", pa.int64()), ("sex", pa.string()),
        ("last_name", pa.string()), ("first_name", pa.string()),
        ("travel_documents", pa.string()), ("n_flights", pa.int32()), ("n_cards", pa.int32()),
    ]))
    cards = Sink(out / "src_json_cards.parquet", pa.schema([
        ("nickname", pa.string()), ("card_idx", pa.int32()),
        ("programm", pa.string()), ("number", pa.string()), ("status", pa.string()),
    ]))
    flights = Sink(out / "src_json_flights.parquet", pa.schema([
        ("nickname", pa.string()), ("flight_idx", pa.int32()),
        ("flight", pa.string()), ("date", pa.string()), ("codeshare", pa.string()),
        ("dep_airport", pa.string()), ("dep_city", pa.string()), ("dep_country", pa.string()),
        ("arr_airport", pa.string()), ("arr_city", pa.string()), ("arr_country", pa.string()),
    ]))
    with open(path, "rb") as f:
        for idx, p in enumerate(ijson.items(f, "Forum Profiles.item"), start=1):
            nick = p.get("NickName")
            real = p.get("Real Name") or {}
            loyalty = p.get("Loyality Programm") or []
            fl = p.get("Registered Flights") or []
            docs = p.get("Travel Documents")
            profiles.add({
                "nickname": nick, "profile_idx": idx, "sex": p.get("Sex"),
                "last_name": real.get("Last Name"), "first_name": real.get("First Name"),
                "travel_documents": json.dumps(docs, default=str, ensure_ascii=False),
                "n_flights": len(fl), "n_cards": len(loyalty),
            })
            for j, c in enumerate(loyalty, start=1):
                cards.add({"nickname": nick, "card_idx": j, "programm": c.get("programm"),
                           "number": c.get("Number"), "status": c.get("Status")})
            for j, x in enumerate(fl, start=1):
                dep, arr = x.get("Departure") or {}, x.get("Arrival") or {}
                cs = x.get("Codeshare")
                flights.add({
                    "nickname": nick, "flight_idx": j, "flight": x.get("Flight"),
                    "date": x.get("Date"), "codeshare": None if cs is None else str(cs).lower(),
                    "dep_airport": dep.get("Airport"), "dep_city": dep.get("City"),
                    "dep_country": dep.get("Country"),
                    "arr_airport": arr.get("Airport"), "arr_city": arr.get("City"),
                    "arr_country": arr.get("Country"),
                })
    return profiles.close(), cards.close(), flights.close()


def extract_xml(path, out):
    users = Sink(out / "src_xml_users.parquet", s("uid", "first", "last", "n_cards", "user_idx"))
    cards = Sink(out / "src_xml_cards.parquet",
                 s("uid", "card_idx", "number", "card_type", "bonusprogramm"))
    acts = Sink(out / "src_xml_activities.parquet",
                s("uid", "card_number", "act_idx", "type", "code", "date",
                  "departure", "arrival", "fare", "extra_tags"))
    known = {"Code", "Date", "Departure", "Arrival", "Fare"}
    idx = 0
    for _, el in ET.iterparse(path, events=("end",)):
        if el.tag != "user":
            continue
        idx += 1
        uid = el.get("uid")
        name = el.find("name")
        cl = el.findall("./cards/card")
        users.add({"uid": uid, "first": name.get("first") if name is not None else None,
                   "last": name.get("last") if name is not None else None,
                   "n_cards": str(len(cl)), "user_idx": str(idx)})
        j = 0
        for ci, card in enumerate(cl, start=1):
            bp = card.find("bonusprogramm")
            cards.add({"uid": uid, "card_idx": str(ci), "number": card.get("number"),
                       "card_type": el.find("cards").get("type"),
                       "bonusprogramm": bp.text if bp is not None else None})
            for a in card.findall("./activities/activity"):
                j += 1
                g = {c.tag: c.text for c in a}
                extra = sorted(set(g) - known)
                acts.add({"uid": uid, "card_number": card.get("number"), "act_idx": str(j),
                          "type": a.get("type"), "code": g.get("Code"), "date": g.get("Date"),
                          "departure": g.get("Departure"), "arrival": g.get("Arrival"),
                          "fare": g.get("Fare"), "extra_tags": ",".join(extra) or None})
        el.clear()
    return users.close(), cards.close(), acts.close()


YAML_DATE = re.compile(r"^'?(\d{4}-\d{2}-\d{2})'?:$")
YAML_FLIGHT = re.compile(r"^  ([^ :]+):$")
YAML_KV = re.compile(r"^    (FROM|TO|STATUS): ?(.*)$")
YAML_CARD = re.compile(r"^      (.+?): \{CLASS: ([^,]*), FARE: ([^}]*)\}$")


def extract_yaml(path, out):
    sink = Sink(out / "src_yaml.parquet",
                s("date", "flight", "card", "class", "fare", "from", "to", "status", "line_no"))
    date = flight = None
    pending, meta = [], {}
    unparsed = 0

    def emit():
        for card, cls, fare, ln in pending:
            sink.add({"date": date, "flight": flight, "card": card, "class": cls, "fare": fare,
                      "from": meta.get("FROM"), "to": meta.get("TO"),
                      "status": meta.get("STATUS"), "line_no": str(ln)})

    with open(path, encoding="utf-8") as f:
        for ln, line in enumerate(f, start=1):
            line = line.rstrip("\n")
            if m := YAML_CARD.match(line):
                pending.append((m[1], m[2], m[3], ln))
            elif m := YAML_KV.match(line):
                meta[m[1]] = m[2]
            elif line == "    FF:":
                pass
            elif m := YAML_FLIGHT.match(line):
                emit()
                flight, pending, meta = m[1], [], {}
            elif m := YAML_DATE.match(line):
                emit()
                date, flight, pending, meta = m[1], None, [], {}
            else:
                unparsed += 1
        emit()
    return sink.close(), unparsed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="Airlines")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--only", choices=["json", "xml", "yaml"])
    a = ap.parse_args()
    src, out = Path(a.src), Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    if a.only in (None, "yaml"):
        print("yaml rows, unparsed lines:", extract_yaml(src / "SkyTeam-Exchange.yaml", out))
    if a.only in (None, "xml"):
        print("xml users, cards, activities:", extract_xml(src / "PointzAggregator-AirlinesData.xml", out))
    if a.only in (None, "json"):
        print("json profiles, cards, flights:", extract_json(src / "FrequentFlyerForum-Profiles.json", out))


if __name__ == "__main__":
    main()
