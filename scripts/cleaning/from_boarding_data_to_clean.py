import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


def process_boarding_data(input_path, output_path):
    print(f"Чтение файла: {input_path}")

    df = pd.read_parquet(input_path)

    df.replace({'Not presented': None, 'None': None, '': None}, inplace=True)

    df['birth_date'] = pd.to_datetime(df['PassengerBirthDate'], format='%m/%d/%Y', errors='coerce').dt.date
    df['flight_date'] = pd.to_datetime(df['FlightDate'], unit='ms').dt.date

    df['airline_code'] = df['FlightNumber'].str.extract(r'^([A-Z0-9]{2})', expand=False)
    df['flight_number_clean'] = df['FlightNumber'].str.replace(r'^([A-Z0-9]{2})(\d+)$', r'\1 \2', regex=True)

    df['gender'] = df['PassengerSex'].map({'Male': 'M', 'Female': 'F'}).fillna('U')

    df['codeshare_bool'] = df['CodeShare'].str.lower().map({'own': False, 'codeshare': True, 'true': True}).fillna(
        False
    )

    print(df[['PassengerFirstName', 'PassengerLastName', 'birth_date', 'flight_date', 'FlightNumber',
              'airline_code']].head())

    df.to_parquet(output_path)
    print(f"Данные сохранены в {output_path}")


if __name__ == '__main__':
    process_boarding_data(
        input_path=r'C:\Users\Grigosha\Downloads\BoardingData.parquet',
        output_path=r'BoardingData_clean.parquet'
    )