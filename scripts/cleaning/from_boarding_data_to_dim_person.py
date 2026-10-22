import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


def create_dim_person_from_clean(clean_input_path, parquet_output_path):
    print(f"Чтение очищенного файла: {clean_input_path}")

    df_raw = pd.read_parquet(clean_input_path)
    print(f"Исходных строк: {len(df_raw)}")

    df_raw['first_name'] = df_raw['PassengerFirstName']
    df_raw['last_name'] = df_raw['PassengerLastName']
    df_raw['middle_name'] = df_raw['PassengerSecondName']

    df_raw['gender'] = df_raw['PassengerSex'].map({'Male': 'M', 'Female': 'F'}).fillna('U')

    df_raw['passport'] = df_raw['PassengerDocument']

    def parse_birth_date(val):
        if pd.isna(val):
            return None
        if isinstance(val, (int, float)):
            return pd.to_datetime(val, unit='ms').date()
        return pd.to_datetime(val).date()

    df_raw['birth_date'] = df_raw['birth_date'].apply(parse_birth_date)

    df_raw['unique_key'] = (
            df_raw['last_name'].fillna('').str.lower() + "_" +
            df_raw['first_name'].fillna('').str.lower() + "_" +
            df_raw['middle_name'].fillna('').str.lower() + "_" +
            df_raw['birth_date'].astype(str)
    )

    df_person = df_raw.drop_duplicates(subset=['unique_key']).copy()
    print(f"Уникальных пассажиров найдено: {len(df_person)}")

    df_person.reset_index(drop=True, inplace=True)
    df_person['person_id'] = df_person.index + 1

    df_person['passport_country'] = None
    df_person['ff_airline'] = None
    df_person['ff_number'] = None
    df_person['ff_program_name'] = None
    df_person['forum_uid'] = None
    df_person['forum_registered_flights_count'] = None
    df_person['contact_phone'] = None
    df_person['contact_email'] = None
    df_person['source_file'] = 'BoardingData.parquet'

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

    print(f"Сохранение {len(df_final)} записей в {parquet_output_path}...")

    output_dir = os.path.dirname(parquet_output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

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

    df_final = df_final.astype('object').where(pd.notnull(df_final), None)
    table = pa.Table.from_pandas(df_final, schema=schema, preserve_index=False)

    table = table.set_column(4, 'birth_date', pa.array(
        [x if isinstance(x, pd.Timestamp) or x is None else pd.Timestamp(x).date() for x in df_final['birth_date']],
        type=pa.date32()
    ))

    pq.write_table(table, parquet_output_path)

    print(df_final.head())


if __name__ == '__main__':
    create_dim_person_from_clean(
        clean_input_path=r'BoardingData_clean.parquet',
        parquet_output_path=r'dim_person.parquet'
    )