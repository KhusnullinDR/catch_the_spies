import os

import numpy as np
import pandas as pd

SRC = "boarding_passes.parquet"

df = pd.read_parquet(SRC)
n = len(df)
df = df.reset_index(drop=True)

df["segment_id"] = np.arange(1, n + 1, dtype="int64")
df["person_id"] = np.arange(1, n + 1, dtype="int64")
df["source_file"] = SRC
seq = df["sequence"].astype("string") if "sequence" in df.columns else pd.Series([pd.NA] * n, dtype="string")
sheet = df["source_sheet"].astype("string") if "source_sheet" in df.columns else pd.Series([pd.NA] * n, dtype="string")
df["source_row_id"] = sheet.fillna("") + ":" + seq.fillna("")


def col(df, *candidates, default=pd.NA):
    lookup = {c.strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in lookup:
            return df[lookup[key]]
    return pd.Series([default] * len(df), index=df.index)


def epoch_ms_to_date(s):
    try:
        if pd.isna(s):
            return pd.NaT
        return pd.to_datetime(int(s), unit="ms").date()
    except (ValueError, TypeError):
        return pd.NaT


def parse_flight(flight_raw):
    if pd.isna(flight_raw):
        return pd.NA, pd.NA, pd.NA
    s = str(flight_raw).strip().upper().replace(" ", "")
    i = 0
    while i < len(s) and s[i].isalpha():
        i += 1
    airline = s[:i] or pd.NA
    num = s[i:] or pd.NA
    return airline, num, s


def parse_ff(ff_raw):
    if pd.isna(ff_raw):
        return pd.NA, pd.NA
    s = str(ff_raw).strip()
    if s.lower() in ("null", "none", ""):
        return pd.NA, pd.NA
    parts = s.replace("-", " ").split()
    if len(parts) >= 2 and parts[0].isalpha():
        return parts[0].upper(), "".join(parts[1:])
    if len(s) > 2:
        return s[:2].upper(), s[2:].strip()
    return pd.NA, s


def parse_name(passenger_name):
    if pd.isna(passenger_name):
        return pd.NA, pd.NA, pd.NA
    tokens = str(passenger_name).strip().split()
    if not tokens:
        return pd.NA, pd.NA, pd.NA
    if len(tokens) == 1:
        return tokens[0], pd.NA, pd.NA
    if len(tokens) == 2:
        return tokens[0], pd.NA, tokens[1]
    if len(tokens) == 3:
        return tokens[0], tokens[1], tokens[2]
    return tokens[0], tokens[1], " ".join(tokens[2:])


def title_to_gender(t):
    if pd.isna(t):
        return pd.NA
    t = str(t).strip().upper()
    if t in ("MR",):
        return "M"
    if t in ("MRS", "MS", "MISS"):
        return "F"
    return pd.NA


df["flight_date_parsed"] = col(df, "flight_date").apply(epoch_ms_to_date)

flight_parsed = col(df, "flight").apply(parse_flight)
df["airline_code"] = flight_parsed.apply(lambda x: x[0])
df["flight_number_clean"] = flight_parsed.apply(lambda x: x[1])
df["flight_number_raw"] = flight_parsed.apply(lambda x: x[2])

ff_parsed = col(df, "ff_number").apply(parse_ff)
df["ff_airline"] = ff_parsed.apply(lambda x: x[0])
df["ff_number_clean"] = ff_parsed.apply(lambda x: x[1])

name_parsed = col(df, "passenger_name").apply(parse_name)
df["first_name"] = name_parsed.apply(lambda x: x[0])
df["middle_name"] = name_parsed.apply(lambda x: x[1])
df["last_name"] = name_parsed.apply(lambda x: x[2])

df["gender_parsed"] = col(df, "title").apply(title_to_gender)

fact = pd.DataFrame({
    "segment_id": df["segment_id"],
    "person_id": df["person_id"],
    "source_file": df["source_file"],
    "source_row_id": df["source_row_id"],
    "flight_number_raw": df["flight_number_raw"].astype("string"),
    "flight_number_clean": df["flight_number_clean"].astype("string"),
    "airline_code": df["airline_code"].astype("string"),
    "flight_date": df["flight_date_parsed"],
    "dep_time_actual": col(df, "departure_time").astype("string"),
    "arr_time_actual": pd.NA,
    "pnr": col(df, "pnr").astype("string"),
    "e_ticket": col(df, "e_ticket").astype("string"),
    "seat": col(df, "seat").astype("string"),
    "baggage": pd.NA,
    "agent": pd.NA,
    "status": pd.NA,
    "cabin_class": col(df, "class").astype("string"),
    "fare_code_raw": pd.NA,
    "fare_basis_code": pd.NA,
    "fare_conditions": pd.NA,
    "origin_code": col(df, "from_iata").astype("string"),
    "origin_city": col(df, "from_city").astype("string"),
    "origin_country": pd.NA,
    "dest_code": col(df, "to_iata").astype("string"),
    "dest_city": col(df, "to_city").astype("string"),
    "dest_country": pd.NA,
    "codeshare_raw": pd.NA,
    "codeshare_bool": pd.NA,
    "operating_carrier": col(df, "operator").astype("string"),
    "ff_airline": df["ff_airline"].astype("string"),
    "ff_number": df["ff_number_clean"].astype("string"),
    "ff_program_name": pd.NA,
    "miles_earned": pd.NA,
    "activity_type": pd.NA,
    "data_quality_flag": pd.NA,
    "possible_duplicate": False,
})

dim_person = pd.DataFrame({
    "person_id": df["person_id"],
    "last_name": df["last_name"].astype("string"),
    "first_name": df["first_name"].astype("string"),
    "middle_name": df["middle_name"].astype("string"),
    "birth_date": pd.NaT,
    "gender": df["gender_parsed"].astype("string"),
    "passport": pd.NA,
    "passport_country": pd.NA,
    "ff_airline": df["ff_airline"].astype("string"),
    "ff_number": df["ff_number_clean"].astype("string"),
    "ff_program_name": pd.NA,
    "forum_uid": pd.NA,
    "forum_registered_flights_count": pd.NA,
    "contact_phone": pd.NA,
    "contact_email": pd.NA,
    "source_file": df["source_file"],
})

dim_segment = pd.DataFrame({
    "segment_id": df["segment_id"],
    "source_row_id": df["source_row_id"],
    "flight_number_clean": df["flight_number_clean"].astype("string"),
    "airline_code": df["airline_code"].astype("string"),
    "flight_date": df["flight_date_parsed"],
    "source_file": df["source_file"],
})

OUT_DIR = "boarding_passes_out"
os.makedirs(OUT_DIR, exist_ok=True)

fact.to_parquet(f"{OUT_DIR}/fact_flights.parquet", index=False)
dim_person.to_parquet(f"{OUT_DIR}/dim_person.parquet", index=False)
dim_segment.to_parquet(f"{OUT_DIR}/dim_segment.parquet", index=False)

print(f"fact_flights: {len(fact)} строк")
print(f"dim_person:   {len(dim_person)} строк")
print(f"dim_segment:  {len(dim_segment)} строк")

print("source_row_id уникален:", fact["source_row_id"].is_unique)
print("person_id уникален:", dim_person["person_id"].is_unique)
print("segment_id уникален:", dim_segment["segment_id"].is_unique)
print("Все person_id есть в fact:", set(dim_person.person_id).issubset(set(fact.person_id)))
print("Все segment_id есть в fact:", set(dim_segment.segment_id).issubset(set(fact.segment_id)))

print("\nПример разбора ff_number:")
print(fact[["ff_airline", "ff_number"]].dropna().head(10).to_string(index=False))

print("\nПример разбора ФИО:")
print(dim_person[["last_name", "first_name", "middle_name"]].head(10).to_string(index=False))