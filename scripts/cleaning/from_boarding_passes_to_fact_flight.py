import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


def process_to_fact_flights(parquet_input_path, parquet_output_path):
    print(f"Чтение исходного файла: {parquet_input_path}")

    columns_to_read = [
        'sequence', 'source_sheet', 'source_file', 'flight', 'flight_date',
        'departure_time', 'pnr', 'e_ticket', 'class', 'from_iata',
        'from_city', 'to_iata', 'to_city', 'operator', 'ff_number'
    ]

    df_raw = pd.read_parquet(parquet_input_path, columns=columns_to_read)
    print(f"Всего строк (билетов): {len(df_raw)}")

    df_raw.replace({'null': None, 'NULL': None, '': None}, inplace=True)

    df_final = pd.DataFrame()

    df_final['segment_id'] = range(1, len(df_raw) + 1)
    df_final['person_id'] = None
    df_final['source_file'] = df_raw['source_file']
    df_final['source_row_id'] = df_raw['source_sheet'] + ":" + df_raw['sequence'].astype(str)

    df_final['flight_number_raw'] = df_raw['flight']
    df_final['flight_number_clean'] = df_raw['flight'].str.replace(r'^([A-Z0-9]{2})(\d+)$', r'\1 \2',
                                                                   regex=True).str.strip()
    df_final['airline_code'] = df_raw['flight'].str.extract(r'^([A-Z0-9]{2})', expand=False)

    df_final['flight_date'] = pd.to_datetime(df_raw['flight_date'], unit='ms').dt.normalize()

    df_final['dep_time_actual'] = df_raw['departure_time']
    df_final['arr_time_actual'] = None

    df_final['pnr'] = df_raw['pnr']
    df_final['e_ticket'] = df_raw['e_ticket'].astype(str).replace('None', None)
    df_final['seat'] = None
    df_final['baggage'] = None
    df_final['agent'] = df_raw['operator'].replace('N/A', None)
    df_final['status'] = None

    df_final['cabin_class'] = df_raw['class']
    df_final['fare_code_raw'] = None
    df_final['fare_basis_code'] = None
    df_final['fare_conditions'] = None

    df_final['origin_code'] = df_raw['from_iata']
    df_final['origin_city'] = df_raw['from_city']
    df_final['origin_country'] = None
    df_final['dest_code'] = df_raw['to_iata']

    df_final['dest_city'] = df_raw['to_city'].str.replace(r'\s+[A-Z]{2}$', '', regex=True)
    df_final['dest_country'] = None

    df_final['codeshare_raw'] = None
    df_final['codeshare_bool'] = None
    df_final['operating_carrier'] = None

    df_final['ff_airline'] = df_raw['ff_number'].str.extract(r'^([A-Z]{2})\s?\d+', expand=False)
    df_final['ff_number'] = df_raw['ff_number'].str.extract(r'^[A-Z]{2}\s?(\d+)', expand=False)
    df_final['ff_program_name'] = None
    df_final['miles_earned'] = None
    df_final['activity_type'] = 'Flight'

    df_final['data_quality_flag'] = None
    df_final.loc[df_raw['to_city'].str.contains(r'\s+[A-Z]{2}$', na=False), 'data_quality_flag'] = 'city_with_state'

    df_final['possible_duplicate'] = False

    print(f"Сохранение {len(df_final)} записей в {parquet_output_path}...")

    output_dir = os.path.dirname(parquet_output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

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

    table = table.set_column(7, 'flight_date',
                             pa.array([x.date() if pd.notnull(x) else None for x in df_final['flight_date']],
                                      type=pa.date32()))

    pq.write_table(table, parquet_output_path)

    print(df_final[['segment_id', 'flight_date', 'flight_number_clean', 'origin_code', 'dest_code']].head())


if __name__ == '__main__':
    process_to_fact_flights(
        parquet_input_path=r'boarding_passes.parquet',
        parquet_output_path=r'fact_flights.parquet'
    )