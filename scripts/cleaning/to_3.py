import pandas as pd
import numpy as np
from datetime import datetime

SRC = "BoardingData_clean.parquet"

df = pd.read_parquet(SRC)
n = len(df)

df = df.reset_index(drop=True)
df["segment_id"] = np.arange(1, n + 1, dtype="int64")
df["person_id"] = np.arange(1, n + 1, dtype="int64")
df["source_row_id"] = df["segment_id"].astype(str)
df["source_file"] = SRC


def col(df, *candidates, default=pd.NA):
    lookup = {c.strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in lookup:
            return df[lookup[key]]
    return pd.Series([default] * len(df), index=df.index)


def to_date(s):
    if pd.isna(s):
        return pd.NaT
    s = str(s).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return pd.NaT


def epoch_ms_to_date(s):
    try:
        if pd.isna(s):
            return pd.NaT
        return pd.to_datetime(int(s), unit="ms").date()
    except (ValueError, TypeError):
        return pd.NaT


def to_int(s):
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return np.nan


df["birth_date_parsed"] = col(df, "PassengerBirthDate").apply(to_date)
mask_bd = df["birth_date_parsed"].isna()
if "birth_date" in df.columns:
    df.loc[mask_bd, "birth_date_parsed"] = col(df, "birth_date")[mask_bd].apply(epoch_ms_to_date)

df["flight_date_parsed"] = col(df, "flight_date").apply(epoch_ms_to_date)
mask_fd = df["flight_date_parsed"].isna()
df.loc[mask_fd, "flight_date_parsed"] = col(df, "FlightDate")[mask_fd].apply(epoch_ms_to_date)

fact = pd.DataFrame({
    "segment_id": df["segment_id"],
    "person_id": df["person_id"],
    "source_file": df["source_file"],
    "source_row_id": df["source_row_id"],
    "flight_number_raw": col(df, "FlightNumber").astype("string"),
    "flight_number_clean": col(df, "flight_number_clean").astype("string"),
    "airline_code": col(df, "airline_code").astype("string"),
    "flight_date": df["flight_date_parsed"],
    "dep_time_actual": col(df, "FlightTime").astype("string"),
    "arr_time_actual": pd.NA,
    "pnr": col(df, "BookingCode").astype("string"),
    "e_ticket": col(df, "TicketNumber").astype("string"),
    "seat": pd.NA,
    "baggage": col(df, "Baggage").astype("string"),
    "agent": pd.NA,
    "status": pd.NA,
    "cabin_class": pd.NA,
    "fare_code_raw": pd.NA,
    "fare_basis_code": pd.NA,
    "fare_conditions": pd.NA,
    "origin_code": pd.NA,
    "origin_city": pd.NA,
    "origin_country": pd.NA,
    "dest_code": pd.NA,
    "dest_city": col(df, "Destination").astype("string"),
    "dest_country": pd.NA,
    "codeshare_raw": col(df, "CodeShare").astype("string"),
    "codeshare_bool": col(df, "codeshare_bool").astype("boolean"),
    "operating_carrier": pd.NA,
    "ff_airline": pd.NA,
    "ff_number": pd.NA,
    "ff_program_name": pd.NA,
    "miles_earned": pd.NA,
    "activity_type": pd.NA,
    "data_quality_flag": pd.NA,
    "possible_duplicate": False,
})

dim_person = pd.DataFrame({
    "person_id": df["person_id"],
    "last_name": col(df, "PassengerLastName").astype("string"),
    "first_name": col(df, "PassengerFirstName").astype("string"),
    "middle_name": col(df, "PassengerSecondName").astype("string"),
    "birth_date": df["birth_date_parsed"],
    "gender": col(df, "gender").astype("string"),
    "passport": col(df, "PassengerDocument").astype("string"),
    "passport_country": pd.NA,
    "ff_airline": pd.NA,
    "ff_number": pd.NA,
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
    "flight_number_clean": col(df, "flight_number_clean").astype("string"),
    "airline_code": col(df, "airline_code").astype("string"),
    "flight_date": df["flight_date_parsed"],
    "source_file": df["source_file"],
})

fact.to_parquet("fact_flights.parquet", index=False)
dim_person.to_parquet("dim_person.parquet", index=False)
dim_segment.to_parquet("dim_segment.parquet", index=False)

print(f"fact_flights: {len(fact)} строк")
print(f"dim_person:   {len(dim_person)} строк")
print(f"dim_segment:  {len(dim_segment)} строк")

print("segment_id -> один person_id в fact:",
      (fact.groupby("segment_id")["person_id"].nunique() == 1).all())

print("source_row_id уникален в fact:",
      fact["source_row_id"].is_unique)

print("Все person_id из dim_person есть в fact:",
      set(dim_person["person_id"]).issubset(set(fact["person_id"])))

print("Все segment_id из dim_segment есть в fact:",
      set(dim_segment["segment_id"]).issubset(set(fact["segment_id"])))