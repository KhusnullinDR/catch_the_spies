import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


def create_fact_flights(clean_input_path, parquet_output_path):
    print(f"Чтение очищенного файла: {clean_input_path}")
    df_raw = pd.read_parquet(clean_input_path)
    print(f"Исходных строк: {len(df_raw)}")

    df_raw.columns = df_raw.columns.str.strip()

    df_final = pd.DataFrame()

    df_final['segment_id'] = range(1, len(df_raw) + 1)
    df_final['person_id'] = None
    df_final['source_file'] = 'BoardingData.parquet'
    df_final['source_row_id'] = pd.Series(range(1, len(df_raw) + 1)).astype(str)

    df_final['flight_number_raw'] = df_raw['FlightNumber']
    df_final['flight_number_clean'] = df_raw['flight_number_clean']
    df_final['airline_code'] = df_raw['airline_code']

    def parse_date(val):
        if pd.isna(val):
            return None
        if isinstance(val, (int, float)):
            return pd.to_datetime(val, unit='ms').date()
        return pd.to_datetime(val).date()

    df_final['flight_date'] = df_raw['flight_date'].apply(parse_date)

    df_final['dep_time_actual'] = df_raw['FlightTime']
    df_final['arr_time_actual'] = None

    df_final['pnr'] = df_raw['BookingCode']
    df_final['e_ticket'] = df_raw['TicketNumber'].astype(str).replace({'None': None, 'nan': None})
    df_final['seat'] = None
    df_final['baggage'] = df_raw['Baggage']
    df_final['agent'] = None
    df_final['status'] = None

    df_final['cabin_class'] = None
    df_final['fare_code_raw'] = None
    df_final['fare_basis_code'] = None
    df_final['fare_conditions'] = None

    df_final['origin_code'] = None
    df_final['origin_city'] = None
    df_final['origin_country'] = None
    df_final['dest_code'] = None
    df_final['dest_city'] = df_raw['Destination']
    df_final['dest_country'] = None

    df_final['codeshare_raw'] = df_raw['CodeShare']
    df_final['codeshare_bool'] = df_raw['codeshare_bool']
    df_final['operating_carrier'] = None

    df_final['ff_airline'] = None
    df_final['ff_number'] = None
    df_final['ff_program_name'] = None
    df_final['miles_earned'] = None
    df_final['activity_type'] = 'Flight'

    df_final['data_quality_flag'] = None
    df_final['possible_duplicate'] = False

    print(f"Сохранение {len(df_final)} записей в {parquet_output_path}...")

    output_dir = os.path.dirname(parquet_output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    schema = pa.schema([
        ('segment_id', pa.int64()), ('person_id', pa.int64()), ('source_file', pa.string()),
        ('source_row_id', pa.string()),
        ('flight_number_raw', pa.string()), ('flight_number_clean', pa.string()), ('airline_code', pa.string()),
        ('flight_date', pa.date32()), ('dep_time_actual', pa.string()), ('arr_time_actual', pa.string()),
        ('pnr', pa.string()), ('e_ticket', pa.string()), ('seat', pa.string()), ('baggage', pa.string()),
        ('agent', pa.string()), ('status', pa.string()),
        ('cabin_class', pa.string()), ('fare_code_raw', pa.string()), ('fare_basis_code', pa.string()),
        ('fare_conditions', pa.string()),
        ('origin_code', pa.string()), ('origin_city', pa.string()), ('origin_country', pa.string()),
        ('dest_code', pa.string()), ('dest_city', pa.string()), ('dest_country', pa.string()),
        ('codeshare_raw', pa.string()), ('codeshare_bool', pa.bool_()), ('operating_carrier', pa.string()),
        ('ff_airline', pa.string()), ('ff_number', pa.string()), ('ff_program_name', pa.string()),
        ('miles_earned', pa.int32()), ('activity_type', pa.string()),
        ('data_quality_flag', pa.string()), ('possible_duplicate', pa.bool_())
    ])

    df_final = df_final.astype('object').where(pd.notnull(df_final), None)
    table = pa.Table.from_pandas(df_final, schema=schema, preserve_index=False)

    table = table.set_column(7, 'flight_date', pa.array(
        [x if isinstance(x, pd.Timestamp) or x is None else pd.Timestamp(x).date() for x in df_final['flight_date']],
        type=pa.date32()
    ))

    pq.write_table(table, parquet_output_path)
    print(df_final[['segment_id', 'flight_number_clean', 'flight_date', 'dest_city', 'pnr']].head())


if __name__ == '__main__':
    create_fact_flights(
        clean_input_path=r'BoardingData_clean.parquet',
        parquet_output_path=r'fact_flights.parquet'
    )