#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import duckdb
import pandas as pd

JSON_SRC = "FrequentFlyerForum-Profiles.json"
XML_SRC = "PointzAggregator-AirlinesData.xml"
YAML_SRC = "SkyTeam-Exchange.yaml"
TICKETS_SRC = "Tickets/YourBoardingPassDotAero.xlsx"
SIRENA_SRC = "Sirena-export-fixed.tab"
BOARDING_SRC = "BoardingData.csv"

TICKETS_BAD = "case when {c} in ('REA', 'OVA', 'NIA', 'NDS') or ({c} = 'NGO' and {city} = 'KINSHASA') then lower({c}) else {c} end"

TRANSLIT = [("'", ""), ("-", ""), ("SHCH", "SC"), ("TCH", "CH"), ("ZH", "J"), ("KH", "H"), ("TS", "C"), ("TC", "C"),
            ("X", "KS"), ("YA", "IA"), ("YU", "IU"), ("YE", "E"), ("Y", "I"), ("J", "I"), ("II", "I"), ("EE", "E"),
            ("W", "V")]


def name_key(full_name):
    if not full_name:
        return None
    toks = []
    for t in full_name.upper().split():
        if len(t) > 1:
            for x, y in TRANSLIT:
                t = t.replace(x, y)
            toks.append(t)
    return " ".join(sorted(toks)) or None

BAD_IATA = {
    "rea": ("ICN", "Seoul", "South Korea"),
    "ova": ("KIV", "Chisinau", "Moldova"),
    "ngo": ("FIH", "Kinshasa", "Democratic Republic of the Congo"),
    "nia": ("DAR", "Dar Es Salaam", "Tanzania"),
    "nds": ("STT", "Saint Thomas", "U.S. Virgin Islands"),
    "SAR": ("HKG", "Hong Kong", "Hong Kong"),
}

AIRPORT_COUNTRY = {
    "SKD": "Uzbekistan",
    "RUN": "France",
    "ZSE": "France",
    "MYR": "United States",
    "CPC": "Argentina",
    "MFM": "Macau",
}
COUNTRY_GROUP = "case when {c} in ('Hong Kong', 'Macau') then 'SAR' else {c} end"

COUNTRY_FIX = {
    "Ko": "South Korea",
    "Mold": "Moldova",
    "Co": "Democratic Republic of the Congo",
    "Tanza": "Tanzania",
    "Virgin Isla": "U.S. Virgin Islands",
    "Russian": "Russian Federation",
    "United States of": "United States",
    "United States of America": "United States",
    "Spain and": "Spain",
    "Spain and Canary": "Spain",
    "Spain and Canary Islands": "Spain",
    "Dominican": "Dominican Republic",
    "Lao People's": "Laos",
    "Korea, Republic of": "South Korea",
    "Tanzania, United": "Tanzania",
    "Tanzania, United Rep.": "Tanzania",
    "Tanzania, United Rep. of": "Tanzania",
    "Congo, Democratic": "Democratic Republic of the Congo",
    "Virgin Islands, U.S.": "U.S. Virgin Islands",
    "SAR, China": "Hong Kong",
}
AMBIGUOUS_COUNTRY = {"United", ""}

FARE_TYPE = {"FLX": "Flexible", "STN": "Standard", "RST": "Restricted", "GRP": "Group"}


def stable_id(namespace, value):
    raw = f"{namespace}:{value}".encode("utf-8", errors="replace")
    return int.from_bytes(hashlib.sha256(raw).digest()[:8], "big") & ((1 << 63) - 1)


def union_find_clusters(edges):
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in edges:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)
    return {x: find(x) for x in parent}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-dir", default=".")
    ap.add_argument("--src-dir", default="work/src", help="Выход extract_sources.py")
    ap.add_argument("--out-dir", default="output")
    ap.add_argument("--schedule-year-shift", type=int, default=1,
                    help="Запасной сдвиг года для полётов без точного совпадения с расписанием (0 = только строго по датам)")
    a = ap.parse_args()
    ind, src, out = Path(a.in_dir), Path(a.src_dir), Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.sql("set memory_limit='3GB'; set preserve_insertion_order=false")
    con.create_function("name_key", name_key, ["VARCHAR"], "VARCHAR")
    sql = con.sql

    dup_uid = "708343118"
    old_pid = stable_id("xml-person", dup_uid)
    new_pid = stable_id("xml-person", f"{dup_uid}#2")
    sql(f"""create table person as
        select case when person_id={old_pid} and last_name='KONDRATOV' then {new_pid} else person_id end person_id,
               * exclude(person_id, birth_date, forum_registered_flights_count),
               -- после pandas-обработки типы съехали (INT/DOUBLE); возвращаем по fields.txt
               cast(null as date) birth_date,
               cast(forum_registered_flights_count as integer) forum_registered_flights_count
        from '{ind}/dim_person.parquet'""")
    assert sql("select count(*) - count(distinct person_id) from person").fetchone()[0] == 0

    nick = sql(f"select distinct nickname from '{src}/src_json_cards.parquet'").df()
    nick["person_id"] = nick.nickname.map(lambda v: stable_id("forum-person", v))
    xu = sql(f"""select uid, last, row_number() over (partition by uid order by user_idx::int) k
                 from '{src}/src_xml_users.parquet'""").df()
    xu["person_id"] = [stable_id("xml-person", u if k == 1 else f"{u}#{k}") for u, k in zip(xu.uid, xu.k)]
    con.register("nick_map", nick)
    con.register("xml_map", xu)
    sql(f"""create table bridge as
        with j as (
            select m.person_id, 'JSON' src, c.programm ff_airline, trim(c.number) ff_number,
                   c.status ff_status, c.card_idx
            from '{src}/src_json_cards.parquet' c join nick_map m using(nickname)),
        x as (
            -- в src_xml_cards uid повторяется у дубля; карты есть только у второго (KONDRATOV)
            select m.person_id, 'XML' src, split_part(c.number, ' ', 1), split_part(c.number, ' ', 2),
                   null, c.card_idx::int
            from '{src}/src_xml_cards.parquet' c
            join xml_map m on m.uid = c.uid
                and m.k = (select max(k) from xml_map m2 where m2.uid = c.uid)),
        u as (select * from j union all select * from x),
        prog as (select distinct split_part(number, ' ', 1) ff_airline, bonusprogramm ff_program_name
                 from '{src}/src_xml_cards.parquet')
        select u.*, prog.ff_program_name,
               -- одна карта у нескольких профилей ОДНОГО источника = коллизия, для связей не используем
               count(*) over (partition by u.src, u.ff_airline, u.ff_number) > 1 card_ambiguous
        from u left join prog using(ff_airline)""")

    # Склейка JSON XML
    edges = sql("""select a.person_id, b.person_id from bridge a join bridge b
                   using(ff_airline, ff_number)
                   where a.src='JSON' and b.src='XML'
                     and not a.card_ambiguous and not b.card_ambiguous""").fetchall()
    root = union_find_clusters(edges)
    persons = sql("select person_id from person order by person_id").df()
    persons["root"] = persons.person_id.map(lambda p: root.get(p, p))
    roots = sorted(persons.root.unique())
    rid = {r: i + 1 for i, r in enumerate(roots)}
    persons["passenger_id"] = persons.root.map(rid).astype("int64")
    con.register("pmap_df", persons[["person_id", "passenger_id"]])
    sql("create table pmap as select * from pmap_df")
    sql("create table person2 as select p.*, m.passenger_id from person p join pmap m using(person_id)")
    sql("create table bridge2 as select b.*, m.passenger_id from bridge b join pmap m using(person_id)")

    bad = pd.DataFrame([(k, *v) for k, v in BAD_IATA.items()], columns=["bad", "code", "city", "country"])
    con.register("bad_iata", bad)
    fix_cases = " ".join(f"when '{k.replace("'", "''")}' then '{v}'" for k, v in COUNTRY_FIX.items())
    sql(f"""create table airport_obs as
        select code, city, coalesce(case country {fix_cases} end, country) country from (
            select dep_airport code, dep_city city, dep_country country from '{src}/src_json_flights.parquet'
            union all
            select arr_airport, arr_city, arr_country from '{src}/src_json_flights.parquet')
        where code not in (select bad from bad_iata)""")
    sql(f"""create table dim_airport as
        select code,
               mode(city) city,
               mode(country) filter (where country is not null and country not in ('United','')) country
        from airport_obs group by code""")
    ac_cases = " ".join(f"when '{k}' then '{v}'" for k, v in AIRPORT_COUNTRY.items())
    sql(f"""update dim_airport set country = coalesce(
            case when regexp_matches(city, ' [A-Z]{{2}}$') then 'United States' end,
            case code {ac_cases} end)
        where country is null""")
    sql(f"""create table airports0 as
        select code, city, country from dim_airport where code not in (select code from bad_iata)
        union all select code, city, country from bad_iata
        -- аэропорты, которых нет на форуме, берём из расписания
        union all select code, mode(city), mode(coalesce(case country {fix_cases} end, country)) from (
            select origin_code code, origin_city city, origin_country country from '{ind}/dim_flight_schedule.parquet'
            union all select dest_code, dest_city, dest_country from '{ind}/dim_flight_schedule.parquet')
        where code not in (select code from dim_airport) and code not in (select code from bad_iata)
        group by code""")
    sql(f"""create table airports as
        select code, city, coalesce(case code {ac_cases} end, country) country from airports0""")

    # Объединение
    sql(f"""create table f0 as
        select * from '{ind}/fact_flights.parquet'
        union all by name
        select * exclude (rid) replace (rid as source_row_id,
            -- segment_id в YAML — порядковые 1..N; считаем как stable_id("segment", ...) в create_parquet.py
            (('0x' || left(sha256('segment:{YAML_SRC}:' || rid), 16))::ubigint
                & 9223372036854775807::ubigint)::bigint as segment_id)
        from (select *,
            -- в YAML нет source_row_id; натуральный ключ уникален (проверено)
            strftime(flight_date, '%Y-%m-%d') || ':' || flight_number_raw || ':' || ff_airline || ' ' || ff_number rid
            from '{ind}/fact_flights_yaml.parquet')
        union all by name
        -- Посадочные талоны: fact_flight_extra.parquet сверен с Tickets/*.xlsx по e_ticket (уникален).
        -- В extra потеряны дата полёта и имя файла (source_row_id = лист:SEQUENCE не уникален),
        -- flight_number_clean без кода авиакомпании, типы колонок не по схеме — берём из талона.
        select (('0x' || left(sha256('segment:{TICKETS_SRC}:' || t.file || ':' || t.sheet), 16))::ubigint
                    & 9223372036854775807::ubigint)::bigint segment_id,
               cast(null as bigint) person_id,
               '{TICKETS_SRC}' source_file, t.file || ':' || t.sheet source_row_id,
               t.flight flight_number_raw, t.flight flight_number_clean, left(t.flight, 2) airline_code,
               t.date::date flight_date, t.time::time dep_time_actual, cast(null as time) arr_time_actual,
               t.pnr, t.e_ticket, nullif(t.seat, 'N/A') seat,
               t.cabin_class, t.from_code origin_code, t.to_code dest_code,
               -- "Operated by Some Other Airline" стоит на ВСЕХ 615 771 талонах: признак неинформативен
               t.operated_by codeshare_raw, cast(null as boolean) codeshare_bool,
               split_part(t.ff_card, ' ', 1) ff_airline, split_part(t.ff_card, ' ', 2) ff_number,
               'Boarding' activity_type, false possible_duplicate
        from '{ind}/fact_flight_extra.parquet' e join '{src}/src_tickets.parquet' t using(e_ticket)
        union all by name
        -- Sirena: рейсы SU с документами пассажиров (разбор extract_passenger_docs.py)
        select (('0x' || left(sha256('segment:{SIRENA_SRC}:' || line_no), 16))::ubigint
                    & 9223372036854775807::ubigint)::bigint segment_id,
               cast(null as bigint) person_id, '{SIRENA_SRC}' source_file, line_no::varchar source_row_id,
               flight flight_number_raw, flight flight_number_clean, left(flight, 2) airline_code,
               dep_date::date flight_date, dep_time::time dep_time_actual, arr_time::time arr_time_actual,
               pnr, e_ticket, seat, cabin_class, fare fare_code_raw, baggage, agent,
               codeshare codeshare_raw, codeshare = 'YES' codeshare_bool,
               ff_airline, ff_number, 'Check-in' activity_type, false possible_duplicate
        from '{src}/src_sirena.parquet'
        union all by name
        -- BoardingData: аэропорт вылета не указан, прилёт — только город (часто обрезан: Yuzhno, Khanty)
        select (('0x' || left(sha256('segment:{BOARDING_SRC}:' || line_no), 16))::ubigint
                    & 9223372036854775807::ubigint)::bigint segment_id,
               cast(null as bigint) person_id, '{BOARDING_SRC}' source_file, line_no::varchar source_row_id,
               flight flight_number_raw, flight flight_number_clean, left(flight, 2) airline_code,
               flight_date::date flight_date, flight_time::time dep_time_actual, cast(null as time) arr_time_actual,
               pnr, e_ticket, baggage_status baggage, destination_city dest_city,
               codeshare codeshare_raw, codeshare = 'Operated' codeshare_bool,
               'Boarding' activity_type, false possible_duplicate
        from '{src}/src_boarding.parquet'""")
    assert sql(f"select count(*) from f0 where source_file = '{TICKETS_SRC}'").fetchone()[0] == sql(
        f"select count(*) from '{ind}/fact_flight_extra.parquet'").fetchone()[0]
    assert stable_id("segment", f"{YAML_SRC}:2017-01-01:AF1145:FB 520518073") == sql(
        f"select segment_id from f0 where source_row_id = '2017-01-01:AF1145:FB 520518073'").fetchone()[0]
    assert sql("select count(*) - count(distinct segment_id) from f0").fetchone()[0] == 0

    sql(f"""create table raw_codes as
        select *, o o_key, d d_key from (
            select '{JSON_SRC}' source_file, nickname || ':' || flight_idx source_row_id, dep_airport o, arr_airport d
              from '{src}/src_json_flights.parquet'
            union all
            select '{XML_SRC}', uid || ':' || act_idx, departure, arrival from '{src}/src_xml_activities.parquet'
            union all
            select '{YAML_SRC}', date || ':' || flight || ':' || card, "from", "to" from '{src}/src_yaml.parquet')
        union all
        select '{TICKETS_SRC}', file || ':' || sheet, from_code, to_code,
               {TICKETS_BAD.format(c="from_code", city="from_city")}, {TICKETS_BAD.format(c="to_code", city="to_city")}
        from '{src}/src_tickets.parquet'
        union all
        select '{SIRENA_SRC}', line_no::varchar, origin, dest, origin, dest from '{src}/src_sirena.parquet'
        union all
        select '{BOARDING_SRC}', line_no::varchar, null, null, null, null from '{src}/src_boarding.parquet'""")
    fare_cases = " ".join(f"when '{k}' then '{v}'" for k, v in FARE_TYPE.items())
    sql(f"""create table f1 as
        select f.* exclude(person_id, ff_number, ff_airline, origin_code, dest_code,
                           origin_city, origin_country, dest_city, dest_country,
                           cabin_class, fare_basis_code, fare_conditions, data_quality_flag),
            -- исправление дубля person_id (активности uid 708343118 принадлежат KONDRATOV)
            case when f.source_file='{XML_SRC}' and f.source_row_id like '{dup_uid}:%' then {new_pid}
                 else f.person_id end person_id_src,
            -- в XML номер карты хранился как "DT 857194509"
            f.ff_airline,
            nullif(regexp_replace(f.ff_number, '[^0-9]', '', 'g'), '') ff_number,
            rc.o origin_code_raw, rc.d dest_code_raw,
            coalesce(bo.code, rc.o) origin_code,
            coalesce(bd.code, rc.d) dest_code,
            coalesce(bo.city, f.origin_city, ao.city) origin_city,
            coalesce(bo.country, ao.country) origin_country,
            coalesce(bd.city, f.dest_city, ad.city) dest_city,
            coalesce(bd.country, ad.country) dest_country,
            coalesce(f.cabin_class, left(f.fare_code_raw, 1)) cabin_class,
            f.fare_code_raw fare_basis_code,
            case substr(f.fare_code_raw, 2, 3) {fare_cases} end fare_conditions,
            nullif(concat_ws(';',
                case when bo.bad is not null then 'bad_iata_origin_recovered' end,
                case when bd.bad is not null then 'bad_iata_dest_recovered' end,
                case when f.flight_date is null then 'missing_date' end,
                case when f.cabin_class is not null and f.fare_code_raw is not null
                      and f.cabin_class <> left(f.fare_code_raw, 1) then 'class_fare_mismatch' end), '') data_quality_flag
        from f0 f
        join raw_codes rc using(source_file, source_row_id)
        left join bad_iata bo on bo.bad = rc.o_key
        left join bad_iata bd on bd.bad = rc.d_key
        left join airports ao on ao.code = coalesce(bo.code, rc.o)
        left join airports ad on ad.code = coalesce(bd.code, rc.d)""")
    assert sql("select (select count(*) from f0) - (select count(*) from f1)").fetchone()[0] == 0

    sql("""create table card_owner as
        select ff_airline, ff_number,
               count(distinct passenger_id) n_pass,
               min(passenger_id) passenger_id,
               -- предпочитаем XML-профиль: у него карта стоит в той же структуре, что и в YAML
               arg_min(person_id, case src when 'XML' then 0 else 1 end) person_id
        from bridge2 group by all""")
    sql("""create table f2 as
        select f.* exclude(person_id_src, ff_program_name),
            coalesce(f.person_id_src, case when co.n_pass = 1 then co.person_id end) person_id,
            coalesce(pm.passenger_id, case when co.n_pass = 1 then co.passenger_id end) passenger_id,
            co.n_pass card_owners,
            coalesce(f.ff_program_name, pr.ff_program_name) ff_program_name
        from f1 f
        left join pmap pm on pm.person_id = f.person_id_src
        left join card_owner co using(ff_airline, ff_number)
        left join (select distinct ff_airline, ff_program_name from bridge2 where ff_program_name is not null) pr
            using(ff_airline)""")

    sql("""create table amb as
        select f.segment_id, min(b.passenger_id) passenger_id, min(b.person_id) person_id,
               count(distinct b.passenger_id) n
        from f2 f
        join bridge2 b using(ff_airline, ff_number)
        join f2 g on g.passenger_id = b.passenger_id and g.flight_number_clean = f.flight_number_clean
                 and g.flight_date = f.flight_date and g.source_file <> f.source_file
        where f.passenger_id is null and f.card_owners > 1
        group by 1""")
    sql("""update f2 set passenger_id = amb.passenger_id, person_id = amb.person_id
           from amb where f2.segment_id = amb.segment_id and amb.n = 1""")
    xml_bad = sql(f"""select count(*) from f2 join card_owner co using(ff_airline, ff_number)
                      where source_file='{XML_SRC}' and co.n_pass = 1 and co.passenger_id <> f2.passenger_id""").fetchone()[0]

    sql(f"""create table tk as
        select '{TICKETS_SRC}' source_file, t.file || ':' || t.sheet source_row_id, t.full_name, t.title,
               name_key(t.full_name) nk, p.last_name, p.first_name, p.middle_name
        from '{src}/src_tickets.parquet' t
        join '{ind}/fact_flight_extra.parquet' e using(e_ticket)
        join (select x.sid, d.* from '{ind}/dim_person_extra.parquet' d, unnest(d.id_duplicate) x(sid)) p
          on p.sid = e.segment_id""")
    sql("""create table pname as
        select distinct passenger_id, name_key(last_name || ' ' || first_name) nk from person2
        where last_name is not null and first_name is not null""")
    sql("alter table f2 add column link_method varchar")
    sql(f"""update f2 set link_method = case
            when source_file in ('{JSON_SRC}', '{XML_SRC}') then 'source_profile'
            when passenger_id is not null then 'ff_card' end""")
    sql(f"""create table l2 as
        select t.segment_id, min(g.passenger_id) passenger_id, count(distinct g.passenger_id) n
        from f2 t join tk using(source_file, source_row_id)
        join f2 g on g.flight_number_clean = t.flight_number_clean and g.flight_date = t.flight_date
                 and g.source_file <> '{TICKETS_SRC}'
        join pname pn on pn.passenger_id = g.passenger_id and pn.nk = tk.nk
        where t.source_file = '{TICKETS_SRC}' and t.passenger_id is null
        group by 1""")
    sql("""update f2 set passenger_id = l2.passenger_id, link_method = 'flight_date_name'
           from l2 where f2.segment_id = l2.segment_id and l2.n = 1""")
    sql(f"""create table l3c as
        select t.segment_id, tk.nk, min(g.passenger_id) passenger_id, count(distinct g.passenger_id) n
        from f2 t join tk using(source_file, source_row_id)
        join f2 g on g.flight_number_clean = t.flight_number_clean and g.flight_date = t.flight_date
                 and g.source_file <> '{TICKETS_SRC}'
        where t.source_file = '{TICKETS_SRC}' and t.passenger_id is null
          and g.passenger_id not in (select passenger_id from pname)
        group by 1, 2""")
    sql("""create table l3 as
        with c as (select * from l3c where n = 1),
             s as (select *, sum(k) over (partition by passenger_id) total,
                          row_number() over (partition by passenger_id order by k desc, nk) rk
                   from (select passenger_id, nk, count(*) k from c group by all))
        select c.segment_id, c.passenger_id from c join s using(passenger_id, nk)
        where s.rk = 1 and s.k >= 2 and s.k > 0.5 * s.total""")
    sql("""update f2 set passenger_id = l3.passenger_id, link_method = 'unnamed_passenger_dominant_name'
           from l3 where f2.segment_id = l3.segment_id""")
    max_pid = sql("select max(passenger_id) from person2").fetchone()[0]
    base_max_pid = max_pid
    newp = sql(f"""select tk.nk, tk.title, any_value(tk.last_name) last_name, any_value(tk.first_name) first_name,
                          mode(tk.middle_name) middle_name
                   from f2 t join tk using(source_file, source_row_id)
                   where t.source_file = '{TICKETS_SRC}' and t.passenger_id is null
                   group by 1, 2 order by 1, 2""").df()
    newp["person_id"] = [stable_id("ticket-person", f"{n}|{t}") for n, t in zip(newp.nk, newp.title)]
    newp["passenger_id"] = range(max_pid + 1, max_pid + 1 + len(newp))
    con.register("newp", newp)
    sql(f"""insert into person2 by name
        select person_id, passenger_id, last_name, first_name, middle_name,
               case title when 'MR' then 'M' when 'MRS' then 'F' else 'U' end gender, '{TICKETS_SRC}' source_file
        from newp""")
    sql("insert into pmap select person_id, passenger_id from newp")
    sql(f"""update f2 set passenger_id = n.passenger_id, person_id = n.person_id, link_method = 'name_only_new'
            from tk join newp n using(nk, title)
            where f2.source_file = tk.source_file and f2.source_row_id = tk.source_row_id and f2.passenger_id is null""")
    sql(f"""update f2 set person_id = p.person_id
            from (select passenger_id, min(person_id) person_id from person2 group by 1) p
            where f2.source_file = '{TICKETS_SRC}' and f2.person_id is null and f2.passenger_id = p.passenger_id""")
    sql(f"""create table ticket_names as
        select f2.passenger_id,
               mode(tk.last_name) filter (where f2.link_method = 'unnamed_passenger_dominant_name') last_name,
               mode(tk.first_name) filter (where f2.link_method = 'unnamed_passenger_dominant_name') first_name,
               mode(case tk.title when 'MR' then 'M' when 'MRS' then 'F' end) gender
        from f2 join tk using(source_file, source_row_id)
        where f2.link_method <> 'name_only_new' group by 1""")

    # Sirena и BoardingData
    sql(f"""create table dk as
        select '{SIRENA_SRC}' source_file, line_no::varchar source_row_id, e_ticket,
               name_key(last_name || ' ' || first_name) nk, last_name, first_name, middle_name,
               cast(null as varchar) gender, birth_date::date birth_date, passport,
               regexp_replace(passport, ' ', '', 'g') passport_digits
        from '{src}/src_sirena.parquet'
        union all
        select '{BOARDING_SRC}', line_no::varchar, e_ticket,
               name_key(last_name || ' ' || first_name), upper(last_name), upper(first_name), middle_name,
               case sex when 'Male' then 'M' when 'Female' then 'F' end, birth_date::date, passport,
               regexp_replace(passport, ' ', '', 'g')
        from '{src}/src_boarding.parquet'""")
    sql(f"""update f2 set link_method = 'ff_card'
            where source_file in ('{SIRENA_SRC}', '{BOARDING_SRC}') and passenger_id is not null""")
    DOCS = f"('{SIRENA_SRC}', '{BOARDING_SRC}')"

    def link_by_eticket():
        sql(f"""update f2 set passenger_id = x.passenger_id, link_method = 'e_ticket'
            from (select e_ticket, min(passenger_id) passenger_id from f2
                  where e_ticket is not null and passenger_id is not null group by 1
                  having count(distinct passenger_id) = 1) x
            where f2.source_file in {DOCS} and f2.passenger_id is null and f2.e_ticket = x.e_ticket""")

    link_by_eticket()
    sql(f"""create table l2d as
        select t.segment_id, min(g.passenger_id) passenger_id, count(distinct g.passenger_id) n
        from f2 t join dk using(source_file, source_row_id)
        join f2 g on g.flight_number_clean = t.flight_number_clean and g.flight_date = t.flight_date
                 and g.source_file not in {DOCS}
        join pname pn on pn.passenger_id = g.passenger_id and pn.nk = dk.nk
        where t.source_file in {DOCS} and t.passenger_id is null
        group by 1""")
    sql("""update f2 set passenger_id = l2d.passenger_id, link_method = 'flight_date_name'
           from l2d where f2.segment_id = l2d.segment_id and l2d.n = 1""")
    link_by_eticket()
    sql(f"""update f2 set passenger_id = x.passenger_id, link_method = 'passport_name'
        from dk join (select dk.passport_digits, dk.nk, min(f2.passenger_id) passenger_id
                      from f2 join dk using(source_file, source_row_id) where f2.passenger_id is not null
                      group by 1, 2 having count(distinct f2.passenger_id) = 1) x using(passport_digits, nk)
        where f2.source_file = dk.source_file and f2.source_row_id = dk.source_row_id and f2.passenger_id is null""")
    link_by_eticket()
    sql("""create table ident as
        select f2.passenger_id,
               coalesce(mode(dk.birth_date) filter (where f2.link_method in ('ff_card', 'e_ticket')),
                        mode(dk.birth_date)) bd,
               coalesce(mode(left(dk.middle_name, 1)) filter (where f2.link_method in ('ff_card', 'e_ticket')),
                        mode(left(dk.middle_name, 1))) mi
        from f2 join dk using(source_file, source_row_id) where f2.passenger_id is not null group by 1""")
    detached = sql(f"""select count(*) from f2 join dk using(source_file, source_row_id) join ident using(passenger_id)
        where f2.link_method in ('flight_date_name', 'passport_name')
          and (dk.birth_date <> ident.bd or left(dk.middle_name, 1) <> ident.mi)""").fetchone()[0]
    sql(f"""update f2 set passenger_id = null, person_id = null, link_method = null
        from (select f2.segment_id from f2 join dk using(source_file, source_row_id) join ident i using(passenger_id)
              where f2.link_method in ('flight_date_name', 'passport_name')
                and (dk.birth_date <> i.bd or left(dk.middle_name, 1) <> i.mi)) x
        where f2.segment_id = x.segment_id""")
    rest = sql(f"""select f2.segment_id, dk.passport_digits || '|' || dk.nk pk, f2.e_ticket
                   from f2 join dk using(source_file, source_row_id) where f2.passenger_id is null""").df()
    edges = [(f"seg:{sg}", f"pk:{pk}") for sg, pk in zip(rest.segment_id, rest.pk)]
    edges += [(f"seg:{sg}", f"et:{et}") for sg, et in zip(rest.segment_id, rest.e_ticket) if isinstance(et, str)]
    root = union_find_clusters(edges)
    rest["grp"] = [root[f"seg:{sg}"] for sg in rest.segment_id]
    grp_ids = {g: i for i, g in enumerate(sorted(rest.grp.unique()))}
    max_pid = sql("select max(passenger_id) from person2").fetchone()[0]
    rest["passenger_id"] = rest.grp.map(lambda g: max_pid + 1 + grp_ids[g]).astype("int64")
    rest["person_id"] = rest.grp.map(lambda g: stable_id("doc-person", g)).astype("int64")
    con.register("rest_df", rest[["segment_id", "passenger_id", "person_id"]])
    sql("""update f2 set passenger_id = r.passenger_id, person_id = r.person_id, link_method = 'document_new'
           from rest_df r where f2.segment_id = r.segment_id""")
    sql(f"""insert into person2 by name
        select r.person_id, r.passenger_id, mode(dk.last_name) last_name, mode(dk.first_name) first_name,
               mode(dk.middle_name) middle_name, coalesce(mode(dk.gender), 'U') gender,
               min(dk.birth_date) birth_date, mode(dk.passport) passport, 'RU' passport_country,
               string_agg(distinct f2.source_file, ',') source_file
        from rest_df r join f2 using(segment_id) join dk using(source_file, source_row_id)
        group by r.person_id, r.passenger_id""")
    sql("insert into pmap select distinct person_id, passenger_id from rest_df")
    sql(f"""update f2 set person_id = p.person_id
            from (select passenger_id, min(person_id) person_id from person2 group by 1) p
            where f2.source_file in {DOCS} and f2.person_id is null and f2.passenger_id = p.passenger_id""")
    sql(f"""create table doc_home as
        select passport_digits, nk, arg_max(passenger_id, cnt + 1000000 * profiled::int) home,
               count(*) filter (where profiled) n_profiled
        from (select dk.passport_digits, dk.nk, f2.passenger_id, f2.passenger_id <= {base_max_pid} profiled, count(*) cnt
              from f2 join dk using(source_file, source_row_id) group by 1, 2, 3, 4)
        group by 1, 2""")
    moved_docs = sql(f"""select count(*) from f2 join dk using(source_file, source_row_id) join doc_home h using(passport_digits, nk)
        where f2.passenger_id <> h.home and f2.passenger_id > {base_max_pid} and h.n_profiled <= 1""").fetchone()[0]
    sql(f"""create table doc_move as
        select f2.segment_id, h.home from f2 join dk using(source_file, source_row_id) join doc_home h using(passport_digits, nk)
        where f2.passenger_id <> h.home and f2.passenger_id > {base_max_pid} and h.n_profiled <= 1
        union
        -- талоны с тем же e-ticket, что у записи документа этой пары
        select t.segment_id, h.home from f2 t
        join (select distinct f2.e_ticket, h.home from f2 join dk using(source_file, source_row_id)
              join doc_home h using(passport_digits, nk) where f2.e_ticket is not null and h.n_profiled <= 1) h
          on t.e_ticket = h.e_ticket
        where t.source_file = '{TICKETS_SRC}' and t.passenger_id <> h.home and t.passenger_id > {base_max_pid}""")
    sql("""update f2 set passenger_id = m.home, link_method = link_method || '+passport_consolidated'
           from doc_move m where f2.segment_id = m.segment_id""")
    sql("""update f2 set person_id = p.person_id
           from (select passenger_id, min(person_id) person_id from person2 group by 1) p
           where f2.link_method like '%passport_consolidated' and f2.passenger_id = p.passenger_id""")
    sql(f"""delete from person2 where passenger_id > {base_max_pid}
            and passenger_id not in (select distinct passenger_id from f2 where passenger_id is not null)""")
    sql("delete from pmap where person_id not in (select person_id from person2)")
    n_moved = sql("select count(*) from doc_move").fetchone()[0]

    sql(f"""create table person_docs as
        select f2.passenger_id, dk.passport,
               case when regexp_full_match(dk.passport, '\\d{{4}} \\d{{6}}') then 'internal'
                    when regexp_full_match(dk.passport, '\\d{{2}} \\d{{7}}') then 'foreign' else 'other' end doc_type,
               dk.passport_digits, dk.birth_date, dk.nk, dk.last_name, dk.first_name, dk.middle_name,
               f2.source_file, f2.source_row_id, f2.e_ticket, f2.flight_number_clean, f2.flight_date, f2.link_method
        from f2 join dk using(source_file, source_row_id)""")
    
    sql("""create table doc_names as
        select passenger_id, mode(last_name) last_name, mode(first_name) first_name, mode(middle_name) middle_name,
               mode(birth_date) birth_date
        from person_docs group by 1""")
    sql(f"""create table doc_gender as
        select f2.passenger_id, mode(dk.gender) gender from f2 join dk using(source_file, source_row_id)
        where dk.gender is not null group by 1""")

    sql("""update f2 set data_quality_flag = concat_ws(';', data_quality_flag, 'ambiguous_ff_card_unlinked')
           where passenger_id is null""")

    shift = a.schedule_year_shift
    passes = [(0, "exact_dates")] + ([(shift, f"year_shift_{shift}")] if shift else [])
    sql("create table sched as " + " union all ".join(f"""
        select s.*, {prio} prio, '{kind}' match_kind,
               (valid_from - interval {sh} year)::date d_from, (valid_to - interval {sh} year)::date d_to,
               ao.country origin_country_norm, ad.country dest_country_norm
        from '{ind}/dim_flight_schedule.parquet' s
        left join airports ao on ao.code = s.origin_code
        left join airports ad on ad.code = s.dest_code""" for prio, (sh, kind) in enumerate(passes)))

    def sched_on(flight, date):
        return (f"s.flight_number = {flight} and {date} between s.d_from and s.d_to"
                f" and (s.prio > 0 or contains(s.days_mask, (dayofweek({date}) + 1)::varchar))")

    sql("""create table f3 as
        select *,
            dense_rank() over (order by passenger_id, flight_date, flight_number_clean) trip_id,
            count(*) over w > 1 possible_duplicate_in_source
        from f2
        window w as (partition by source_file, passenger_id, flight_date, flight_number_clean)""")
    sql("update f3 set trip_id = null where passenger_id is null")
    sql("""update f3 set possible_duplicate = possible_duplicate_in_source,
                         data_quality_flag = case when possible_duplicate_in_source
                             then concat_ws(';', data_quality_flag, 'duplicate_in_source') else data_quality_flag end""")

    sql(f"""create table route_votes as
        select f.trip_id, f.origin_code, f.dest_code,
               count(*) votes,
               bool_or(f.source_file = '{YAML_SRC}') from_yaml,
               bool_or(s.schedule_id is not null) in_timetable
        from f3 f
        left join sched s
          on {sched_on("f.flight_number_clean", "f.flight_date")}
         and s.origin_code = f.origin_code and s.dest_code = f.dest_code
        -- у BoardingData кодов аэропортов нет: такие записи за маршрут не голосуют
        where f.trip_id is not null and f.origin_code is not null and f.dest_code is not null
        group by all""")
    sql("""create table route_pick as
        select trip_id,
               arg_max(origin_code, score) origin_code, arg_max(dest_code, score) dest_code,
               count(*) n_routes,
               list(origin_code || '-' || dest_code order by score desc) route_candidates
        from (select *, votes * 4 + in_timetable::int * 2 + from_yaml::int score from route_votes)
        group by trip_id""")
    sql(f"""create table route_city as
        select origin_code code, any_value(origin_city) city, any_value(origin_country) country
        from f3 group by 1
        union select dest_code, any_value(dest_city), any_value(dest_country) from f3 group by 1""")

    sql(f"""create table trips as
        select f.trip_id, f.passenger_id,
            f.flight_number_clean flight_number, any_value(f.airline_code) airline_code, f.flight_date,
            any_value(r.origin_code) origin_code, any_value(r.dest_code) dest_code,
            any_value(r.n_routes) > 1 route_conflict, any_value(r.route_candidates) route_candidates,
            any_value(f.dest_city) filter (where f.source_file = '{BOARDING_SRC}') boarding_dest_city,
            -- приоритет атрибутов: YAML (обмен SkyTeam) > XML (агрегатор) > JSON (форум)
            arg_min(f.cabin_class, case f.source_file when '{YAML_SRC}' then 0 when '{XML_SRC}' then 1 else 2 end)
                filter (where f.cabin_class is not null) cabin_class,
            arg_min(f.fare_basis_code, case f.source_file when '{YAML_SRC}' then 0 else 1 end)
                filter (where f.fare_basis_code is not null) fare_basis_code,
            arg_min(f.fare_conditions, case f.source_file when '{YAML_SRC}' then 0 else 1 end)
                filter (where f.fare_conditions is not null) fare_conditions,
            any_value(f.status) filter (where f.status is not null) status,
            bool_or(f.codeshare_bool) codeshare,
            arg_min(f.ff_airline, case f.source_file when '{YAML_SRC}' then 0 else 1 end)
                filter (where f.ff_number is not null) ff_airline,
            arg_min(f.ff_number, case f.source_file when '{YAML_SRC}' then 0 else 1 end)
                filter (where f.ff_number is not null) ff_number,
            count(distinct f.fare_basis_code) > 1 fare_conflict,
            count(distinct f.ff_airline || f.ff_number) > 1 ff_card_conflict,
            bool_or(f.source_file = '{JSON_SRC}') in_forum,
            bool_or(f.source_file = '{XML_SRC}') in_pointz,
            bool_or(f.source_file = '{YAML_SRC}') in_skyteam,
            count(distinct f.source_file) n_sources,
            bool_or(f.source_file = '{TICKETS_SRC}') in_boarding_pass,
            -- полёт известен только по талону, привязанному к "новому" пассажиру по одному имени
            bool_and(f.link_method = 'name_only_new') name_only_identity,
            count(*) n_records,
            list(f.segment_id order by f.segment_id) segment_ids,
            string_agg(distinct f.data_quality_flag, ';') data_quality_flags
        -- left join: полёты только из BoardingData не имеют кодов аэропортов и голосов за маршрут
        from f3 f left join route_pick r using(trip_id)
        group by f.trip_id, f.passenger_id, f.flight_number_clean, f.flight_date""")
    sql("""create table trips_geo as
        select t.trip_id, t.passenger_id, t.flight_number, t.airline_code, t.flight_date,
               t.origin_code, o.city origin_city, o.country origin_country,
               t.dest_code, coalesce(d.city, t.boarding_dest_city) dest_city,
               -- BoardingData: все 48 городов назначения — российские (рейсы SU по России)
               coalesce(d.country, case when t.boarding_dest_city is not null then 'Russian Federation' end) dest_country,
               t.* exclude(trip_id, passenger_id, flight_number, airline_code, flight_date, origin_code, dest_code,
                           boarding_dest_city)
        from trips t
        left join route_city o on o.code = t.origin_code
        left join route_city d on d.code = t.dest_code""")
    # Заполнение пропусков по расписанию
    sql(f"""create table sched_hits as
        select t.trip_id, t.origin_code t_o, t.dest_code t_d, t.origin_country t_oc, t.dest_country t_dc,
               t.data_quality_flags fl, s.*
        from trips_geo t join sched s on {sched_on("t.flight_number", "t.flight_date")}
        -- сдвинутое расписание используем, только если на точные даты рейса нет
        qualify s.prio = min(s.prio) over (partition by t.trip_id)""")
    grp = lambda col: COUNTRY_GROUP.format(c=col)
    sql(f"""create table iata_fix as
        select trip_id,
               coalesce(case when count(distinct origin_code) filter (where o_ok) = 1
                             then any_value(origin_code) filter (where o_ok) end,
                        case when count(distinct (origin_code, dest_code)) filter (where b_ok) = 1
                             then any_value(origin_code) filter (where b_ok) end) new_origin,
               coalesce(case when count(distinct dest_code) filter (where d_ok) = 1
                             then any_value(dest_code) filter (where d_ok) end,
                        case when count(distinct (origin_code, dest_code)) filter (where b_ok) = 1
                             then any_value(dest_code) filter (where b_ok) end) new_dest
        from (select *,
                fl like '%bad_iata_origin%' and t_o in (select code from bad_iata) and dest_code = t_d
                    and {grp("origin_country_norm")} = {grp("t_oc")} o_ok,
                fl like '%bad_iata_dest%' and t_d in (select code from bad_iata) and origin_code = t_o
                    and {grp("dest_country_norm")} = {grp("t_dc")} d_ok,
                -- оба конца мусорные (внутренние корейские рейсы "rea -> rea" стали ICN -> ICN)
                fl like '%bad_iata_origin%' and fl like '%bad_iata_dest%' and t_o = t_d
                    and t_o in (select code from bad_iata) and origin_code <> dest_code
                    and {grp("origin_country_norm")} = {grp("t_oc")}
                    and {grp("dest_country_norm")} = {grp("t_dc")} b_ok
              from sched_hits where fl like '%bad_iata%')
        group by trip_id""")
    sql("""create table trips_fixed as
        select t.* replace (
                   coalesce(x.new_origin, t.origin_code) as origin_code,
                   coalesce(x.new_dest, t.dest_code) as dest_code,
                   coalesce(ao.city, t.origin_city) as origin_city,
                   coalesce(ad.city, t.dest_city) as dest_city,
                   nullif(concat_ws(';', t.data_quality_flags,
                       case when x.new_origin <> t.origin_code or x.new_dest <> t.dest_code then 'iata_from_schedule' end,
                       case when coalesce(x.new_origin, t.origin_code) = coalesce(x.new_dest, t.dest_code)
                            then 'same_origin_dest' end), '') as data_quality_flags)
        from trips_geo t
        left join iata_fix x using(trip_id)
        left join airports ao on ao.code = x.new_origin
        left join airports ad on ad.code = x.new_dest""")
    sql(f"""create table trip_sched as
        select h.trip_id, arg_min(h.schedule_id, h.schedule_id) schedule_id, any_value(h.match_kind) match_kind
        from sched_hits h join trips_fixed t using(trip_id)
        where h.origin_code = t.origin_code and h.dest_code = t.dest_code
        group by 1""")
    sql(f"""create table trips2 as
        select t.*, b.ff_program_name,
               s.schedule_id, ts.match_kind schedule_match,
               s.dep_time sched_dep_time, s.arr_time sched_arr_time, s.arr_day_offset sched_arr_day_offset,
               s.travel_time_min, s.aircraft_code, s.aircraft_name
        from trips_fixed t
        left join (select distinct ff_airline, ff_program_name from bridge2 where ff_program_name is not null) b
            using(ff_airline)
        left join trip_sched ts using(trip_id)
        left join (select distinct * exclude(prio, match_kind, d_from, d_to) from sched) s
            on s.schedule_id = ts.schedule_id""")
    sql("""update f3 set origin_code = x.new_origin from iata_fix x join trips_geo t using(trip_id)
           where f3.trip_id = x.trip_id and x.new_origin is not null
             and f3.data_quality_flag like '%bad_iata_origin%' and f3.origin_code = t.origin_code""")
    sql("""update f3 set dest_code = x.new_dest from iata_fix x join trips_geo t using(trip_id)
           where f3.trip_id = x.trip_id and x.new_dest is not null
             and f3.data_quality_flag like '%bad_iata_dest%' and f3.dest_code = t.dest_code""")

    sql(f"""create table passenger as
        select passenger_id,
            -- имя из XML (транслитерация как в паспорте), иначе с форума
            coalesce(arg_min(last_name, case source_file when '{XML_SRC}' then 0 else 1 end)
                         filter (where last_name is not null), null) last_name,
            arg_min(first_name, case source_file when '{XML_SRC}' then 0 else 1 end)
                filter (where first_name is not null) first_name,
            any_value(last_name) filter (where source_file = '{JSON_SRC}') last_name_forum,
            any_value(first_name) filter (where source_file = '{JSON_SRC}') first_name_forum,
            coalesce(any_value(gender) filter (where gender <> 'U'), 'U') gender,
            any_value(forum_uid) filter (where forum_uid is not null) forum_uid,
            any_value(person_id) filter (where source_file = '{XML_SRC}') pointz_person_id,
            any_value(person_id) filter (where source_file = '{JSON_SRC}') forum_person_id,
            count(*) n_source_profiles
        from person2 group by passenger_id""")
    sql(f"""create table passenger2 as
        select p.* replace (coalesce(p.last_name, tn.last_name, dn.last_name) as last_name,
                            coalesce(p.first_name, tn.first_name, dn.first_name) as first_name,
                            case when p.gender = 'U' then coalesce(tn.gender, dg.gender, 'U') else p.gender end as gender),
               dn.middle_name, dn.birth_date,
               case when p.last_name is not null then 'profile'
                    when tn.last_name is not null then 'boarding_pass'
                    when dn.last_name is not null then 'document' end name_source,
               p.passenger_id in (select passenger_id from newp) boarding_pass_only,
               p.passenger_id in (select passenger_id from rest_df) document_only,
               d.passports, coalesce(d.n_internal, 0) n_internal_passports, coalesce(d.n_foreign, 0) n_foreign_passports,
               coalesce(d.n_birth_dates, 0) n_birth_dates,
               coalesce(c.n_cards, 0) n_ff_cards, c.ff_cards,
               coalesce(t.n_trips, 0) n_trips, t.first_flight, t.last_flight
        from passenger p
        left join ticket_names tn using(passenger_id)
        left join doc_names dn using(passenger_id)
        left join doc_gender dg using(passenger_id)
        left join (select passenger_id, list(distinct passport order by passport) passports,
                          count(distinct passport) filter (where doc_type = 'internal') n_internal,
                          count(distinct passport) filter (where doc_type = 'foreign') n_foreign,
                          count(distinct birth_date) n_birth_dates
                   from person_docs group by 1) d using(passenger_id)
        left join (select passenger_id, count(distinct (ff_airline, ff_number)) n_cards,
                          list(distinct ff_airline || ' ' || ff_number) ff_cards
                   from bridge2 group by 1) c using(passenger_id)
        left join (select passenger_id, count(*) n_trips, min(flight_date) first_flight,
                          max(flight_date) last_flight from trips2 group by 1) t using(passenger_id)""")

    def write(table, name, order):
        sql(f"copy (select * from {table} order by {order}) to '{out / name}' (format parquet, compression zstd)")

    write("person2", "dim_person_clean.parquet", "person_id")
    write("bridge2", "bridge_person_card.parquet", "passenger_id, src, card_idx")
    write("passenger2", "dim_passenger.parquet", "passenger_id")
    sql("create view f3_out as select * exclude(possible_duplicate_in_source, card_owners) from f3")
    write("f3_out", "fact_flights_all.parquet", "passenger_id, flight_date, flight_number_clean")
    write("trips2", "fact_trips.parquet", "passenger_id, flight_date, flight_number")
    write("person_docs", "fact_passenger_documents.parquet", "passenger_id, flight_date")
    sql(f"copy airports to '{out / 'dim_airport.parquet'}' (format parquet)")

    q = lambda s: sql(s).fetchall()
    print("dim_person rows / passengers:", q("select count(*), count(distinct passenger_id) from person2"))
    print("cluster sizes:", q("select n_source_profiles, count(*) from passenger group by 1 order by 1"))
    print("cards (src, n, ambiguous):", q("select src, count(*), count(*) filter (where card_ambiguous) from bridge2 group by 1"))
    print("facts by source (rows, linked, flagged):",
          q("select source_file, count(*), count(passenger_id), count(data_quality_flag) from f3 group by 1 order by 1"))
    print("XML activities on card of another passenger:", xml_bad)
    print("ambiguous cards resolved by flight match:", q("select count(*) filter (where n=1), count(*) from amb"))
    print("trips:", q("select count(*), sum(n_records) from trips2"))
    print("trips by sources:", q("""select in_forum, in_pointz, in_skyteam, count(*) from trips2
                                    group by all order by 4 desc"""))
    print("conflicts (fare, card):", q("select count(*) filter (where fare_conflict), count(*) filter (where ff_card_conflict) from trips2"))
    print("schedule match (trips, with times, iata fixed):", q("""select count(schedule_id),
        count(sched_dep_time), count(*) filter (where data_quality_flags like '%iata_from_schedule%') from trips2"""))
    print("document records detached as conflicting namesakes:", detached)
    print("records moved by passport consolidation (docs, total):", moved_docs, n_moved)
    print("link methods:", q(f"""select source_file, link_method, count(*) from f3
        where source_file in ('{TICKETS_SRC}', '{SIRENA_SRC}', '{BOARDING_SRC}') group by all order by 1, 3 desc"""))
    print("duplicates in source:", q("select source_file, count(*) from f3 where possible_duplicate group by 1"))


if __name__ == "__main__":
    main()
