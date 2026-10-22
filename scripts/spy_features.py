#!/usr/bin/env python3
import argparse

import duckdb

RU = "Russian Federation"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="output")
    a = ap.parse_args()
    O = a.out_dir
    c = duckdb.connect()
    c.sql("set threads=8; set memory_limit='3GB'")
    sql = c.sql

    sql(f"""create table t as
        select t.*, coalesce(t.sched_dep_time, x.dep) dep,
               origin_country is distinct from dest_country international,
               origin_country = '{RU}' from_ru, dest_country = '{RU}' to_ru
        from '{O}/fact_trips.parquet' t
        left join (select trip_id, min(dep_time_actual) dep from '{O}/fact_flights_all.parquet' group by 1) x
          using(trip_id)""")
    sql(f"create table pax as select * from '{O}/dim_passenger.parquet'")

    sql("""create table seq as
        select passenger_id, trip_id, flight_date, dep, origin_city, dest_city, origin_country, dest_country,
               lag(dest_city) over w prev_dest_city, lag(dest_country) over w prev_dest_country,
               lag(flight_date) over w prev_date
        from t where origin_city is not null
        window w as (partition by passenger_id order by flight_date, dep nulls last, trip_id)""")

    sql(f"""create table cand as
        select t.trip_id, list(distinct ao.city) o_cities, list(distinct ad.city) d_cities,
               list(distinct ao.country) o_countries, list(distinct ad.country) d_countries
        from t, unnest(coalesce(t.route_candidates, [t.origin_code || '-' || t.dest_code])) r(rc)
        left join '{O}/dim_airport.parquet' ao on ao.code = split_part(rc, '-', 1)
        left join '{O}/dim_airport.parquet' ad on ad.code = split_part(rc, '-', 2)
        group by 1""")
    sql("""create table seq2 as
        select s.*, c.o_cities, c.o_countries, lag(c.d_cities) over w prev_d_cities, lag(c.d_countries) over w prev_d_countries
        from seq s join cand c using(trip_id)
        window w as (partition by passenger_id order by flight_date, dep nulls last, trip_id)""")
    sql("""create table f_moves as
        select passenger_id,
               count(*) filter (where prev_dest_city is not null) n_legs,
               count(*) filter (where prev_d_cities is not null and not list_has_any(o_cities, prev_d_cities)) n_city_breaks,
               -- "телепорт" между странами: улетел из страны, где по данным не появлялся
               count(*) filter (where prev_d_countries is not null
                                  and not list_has_any(o_countries, prev_d_countries)) n_country_breaks
        from seq2 group by 1""")
    
    sql("""create table sd as
        select a.passenger_id, a.flight_date, a.origin_city oa, b.origin_city ob, a.dest_city da, b.dest_city db
        from t a join t b on a.passenger_id = b.passenger_id and a.flight_date = b.flight_date and a.trip_id < b.trip_id
        where a.origin_city <> b.origin_city
          and a.dest_city is distinct from b.origin_city and b.dest_city is distinct from a.origin_city""")
    sql("""create table f_impossible as
        select passenger_id, count(*) n_impossible_same_day from sd s
        where not exists (select 1 from t x where x.passenger_id = s.passenger_id and x.flight_date = s.flight_date
                          and ((x.origin_city = s.da and x.dest_city = s.ob) or (x.origin_city = s.db and x.dest_city = s.oa)))
        group by 1""")

    sql(f"""create table f_geo as
        select passenger_id,
               count(*) filter (where international) n_international,
               avg(international::int) share_international,
               count(distinct dest_country) n_countries,
               count(distinct dest_code) n_airports,
               count(distinct dest_country) filter (where dest_country <> '{RU}') n_foreign_countries
        from t group by 1""")
    sql(f"""create table f_short as
        select a.passenger_id, count(distinct a.trip_id) n_short_foreign_trips
        from t a join t b on a.passenger_id = b.passenger_id
             and b.flight_date between a.flight_date and a.flight_date + 3 and b.trip_id <> a.trip_id
        where a.from_ru and not a.to_ru and not b.from_ru and b.to_ru and b.origin_country = a.dest_country
        group by 1""")
    
    sql("""create table rare as select dest_code from t group by 1 having count(distinct passenger_id) < 20""")
    sql("""create table f_rare as select passenger_id, count(*) n_rare_destinations
        from t where dest_code in (select dest_code from rare) group by 1""")

    sql(f"""create table docs as select * from '{O}/fact_passenger_documents.parquet'""")
    sql("""create table shared_pass as   -- один паспорт у людей с разными именами
        select passport_digits, count(distinct nk) n_names, list(distinct last_name || ' ' || first_name) holder_names
        from docs group by 1 having count(distinct nk) > 1""")
    sql("""create table f_docs as
        select passenger_id,
               count(distinct passport) filter (where doc_type = 'internal') n_internal_passports,
               count(distinct passport) filter (where doc_type = 'foreign') n_foreign_passports,
               count(distinct birth_date) n_birth_dates,
               count(distinct nk) n_doc_names,
               bool_or(passport_digits in (select passport_digits from shared_pass)) passport_shared,
               list(distinct passport) passports
        from docs group by 1""")

    sql(f"""create table f_book as
        select f.passenger_id,
               count(*) filter (where f.source_file = 'BoardingData.csv') n_boarding_records,
               count(*) filter (where f.source_file = 'BoardingData.csv' and (f.pnr is null or f.e_ticket is null))
                   n_boarding_no_booking,
               count(distinct f.agent) n_agents,
               count(distinct f.ff_airline || f.ff_number) n_cards_used,
               avg(f.codeshare_bool::int) share_codeshare
        from '{O}/fact_flights_all.parquet' f group by 1""")

    sql(f"""create table features as
        select p.passenger_id, p.last_name, p.first_name, p.middle_name, p.gender, p.birth_date, p.forum_uid,
               p.n_trips, p.boarding_pass_only or p.document_only weak_identity,
               p.n_ff_cards, p.ff_cards,
               m.n_legs, m.n_city_breaks, m.n_country_breaks,
               coalesce(i.n_impossible_same_day, 0) n_impossible_same_day,
               g.n_international, g.share_international, g.n_countries, g.n_foreign_countries, g.n_airports,
               coalesce(s.n_short_foreign_trips, 0) n_short_foreign_trips,
               coalesce(r.n_rare_destinations, 0) n_rare_destinations,
               coalesce(d.n_internal_passports, 0) n_internal_passports,
               coalesce(d.n_foreign_passports, 0) n_foreign_passports,
               coalesce(d.n_birth_dates, 0) n_birth_dates, coalesce(d.n_doc_names, 0) n_doc_names,
               coalesce(d.passport_shared, false) passport_shared, d.passports,
               b.n_boarding_records, b.n_boarding_no_booking, b.n_agents, b.n_cards_used, b.share_codeshare
        from pax p
        left join f_moves m using(passenger_id) left join f_impossible i using(passenger_id)
        left join f_geo g using(passenger_id) left join f_short s using(passenger_id)
        left join f_rare r using(passenger_id) left join f_docs d using(passenger_id)
        left join f_book b using(passenger_id)""")
    sql(f"copy (select * from features order by passenger_id) to '{O}/passenger_features.parquet' (format parquet)")
    sql(f"copy (select * from shared_pass) to '{O}/shared_passports.parquet' (format parquet)")
    print(sql("select count(*) from features").fetchone()[0], "passengers")


if __name__ == "__main__":
    main()
