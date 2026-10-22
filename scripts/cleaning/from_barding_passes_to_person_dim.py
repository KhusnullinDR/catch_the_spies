import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


def process_to_dim_person(parquet_input_path, parquet_output_path):
    print(f"Чтение файла: {parquet_input_path}")

    df_raw = pd.read_parquet(parquet_input_path, columns=['passenger_name', 'title', 'ff_number', 'source_file'])

    print(f"Исходных строк: {len(df_raw)}")

    df_raw.replace({'null': None, 'NULL': None, '': None}, inplace=True)

    split_names = df_raw['passenger_name'].str.split(r'\s+', n=2, expand=True)

    df_raw['first_name'] = split_names[0] if 0 in split_names else None
    df_raw['last_name'] = split_names[1] if 1 in split_names else None
    df_raw['middle_name'] = split_names[2] if 2 in split_names else None

    df_raw['gender'] = 'U'
    df_raw.loc[df_raw['title'].str.upper() == 'MRS', 'gender'] = 'F'
    df_raw.loc[df_raw['title'].str.upper() == 'MR', 'gender'] = 'M'

    df_raw['ff_airline'] = df_raw['ff_number'].str.extract(r'^([A-Z]{2})\s?\d+', expand=False)
    df_raw['ff_number_clean'] = df_raw['ff_number'].str.extract(r'^[A-Z]{2}\s?(\d+)', expand=False)

    df_raw['unique_key'] = df_raw['ff_airline'].fillna('') + "_" + df_raw['ff_number_clean'].fillna('')
    fio_key = df_raw['first_name'].astype(str) + "_" + df_raw['last_name'].astype(str) + "_" + df_raw['middle_name'].astype(str)
    df_raw.loc[df_raw['unique_key'] == "_", 'unique_key'] = fio_key

    df_person = df_raw.drop_duplicates(subset=['unique_key']).copy()
    print(f"Уникальных пассажиров найдено: {len(df_person)}")

    df_person.reset_index(drop=True, inplace=True)
    df_person['person_id'] = df_person.index + 1

    df_person['birth_date'] = None
    df_person['passport'] = None
    df_person['passport_country'] = None
    df_person['ff_program_name'] = None
    df_person['forum_uid'] = None
    df_person['forum_registered_flights_count'] = None
    df_person['contact_phone'] = None
    df_person['contact_email'] = None

    df_person['ff_number'] = df_person['ff_number_clean']

    final_columns = [
        'person_id', 'last_name', 'first_name', 'middle_name', 'birth_date',
        'gender', 'passport', 'passport_country', 'ff_airline', 'ff_number',
        'ff_program_name', 'forum_uid', 'forum_registered_flights_count',
        'contact_phone', 'contact_email', 'source_file'
    ]

    df_final = df_person[final_columns]

    df_final = df_final.astype({
        'person_id': 'int64',
        'last_name': 'string',
        'first_name': 'string',
        'middle_name': 'string',
        'birth_date': 'object',
        'gender': 'string',
        'passport': 'string',
        'passport_country': 'string',
        'ff_airline': 'string',
        'ff_number': 'string',
        'ff_program_name': 'string',
        'forum_uid': 'string',
        'forum_registered_flights_count': 'Int32',
        'contact_phone': 'string',
        'contact_email': 'string',
        'source_file': 'string'
    })

    print(f"Сохранение {len(df_final)} уникальных записей в файл: {parquet_output_path}")

    output_dir = os.path.dirname(parquet_output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    table = pa.Table.from_pandas(df_final, preserve_index=False)

    schema = pa.schema([
        ('person_id', pa.int64()),
        ('last_name', pa.string()),
        ('first_name', pa.string()),
        ('middle_name', pa.string()),
        ('birth_date', pa.date32()),
        ('gender', pa.string()),
        ('passport', pa.string()),
        ('passport_country', pa.string()),
        ('ff_airline', pa.string()),
        ('ff_number', pa.string()),
        ('ff_program_name', pa.string()),
        ('forum_uid', pa.string()),
        ('forum_registered_flights_count', pa.int32()),
        ('contact_phone', pa.string()),
        ('contact_email', pa.string()),
        ('source_file', pa.string())
    ])

    table = table.cast(schema)
    pq.write_table(table, parquet_output_path)

    print(df_final.head())


if __name__ == '__main__':
    process_to_dim_person(
        parquet_input_path=r'boarding_passes.parquet',
        parquet_output_path=r'dim_person.parquet'
    )