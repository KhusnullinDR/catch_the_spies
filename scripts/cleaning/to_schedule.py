import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


def create_dim_flight_schedule(clean_input_path, parquet_output_path):
    print(f"1. Чтение очищенного файла: {clean_input_path}")
    df_raw = pd.read_parquet(clean_input_path)
    print(f"Исходных строк: {len(df_raw)}")

    # Убираем случайные пробелы в названиях колонок
    df_raw.columns = df_raw.columns.str.strip()

    print("2. Формирование полей справочника расписания...")

    # --- ПОДГОТОВКА ДАННЫХ ---
    # Конвертируем дату
    df_raw['flight_date'] = pd.to_datetime(df_raw['flight_date']).dt.date

    # Оставляем только уникальные рейсы
    # Ключ уникальности: номер рейса + дата вылета + время вылета
    df_unique = df_raw.drop_duplicates(subset=['FlightNumber', 'flight_date', 'FlightTime']).copy()
    print(f"Уникальных рейсов найдено: {len(df_unique)}")

    # --- ФОРМИРОВАНИЕ ИТОГОВОЙ ТАБЛИЦЫ ---
    df_final = pd.DataFrame()

    # Генерируем первичный ключ
    df_final['schedule_id'] = range(1, len(df_unique) + 1)

    # Рейс и авиакомпания
    df_final['flight_number'] = df_unique['FlightNumber']
    df_final['airline_code'] = df_unique['airline_code']

    # География (в исходнике нет аэропорта вылета, ставим null)
    df_final['origin_code'] = None
    df_final['origin_city'] = None
    df_final['origin_country'] = None

    df_final['dest_code'] = None  # В исходнике только город
    df_final['dest_city'] = df_unique['Destination']
    df_final['dest_country'] = None

    # Даты действия (так как это разовый перелет в этот день)
    df_final['valid_from'] = df_unique['flight_date']
    df_final['valid_to'] = df_unique['flight_date']

    # Расписание (маска дней и время)
    # Ставим '1234567', так как рейс выполняется в этот день
    df_final['days_mask'] = '1234567'
    df_final['dep_time'] = df_unique['FlightTime']
    df_final['arr_time'] = None  # Нет в исходнике
    df_final['arr_day_offset'] = 0
    df_final['travel_time_min'] = None  # Нет в исходнике

    # Самолет (нет в исходнике)
    df_final['aircraft_code'] = None
    df_final['aircraft_name'] = None

    # Источник
    df_final['source_file'] = 'BoardingData.parquet'

    print(f"3. Сохранение {len(df_final)} записей в {parquet_output_path}...")

    output_dir = os.path.dirname(parquet_output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Схема Parquet
    schema = pa.schema([
        ('schedule_id', pa.int64()),
        ('flight_number', pa.string()),
        ('airline_code', pa.string()),
        ('origin_code', pa.string()),
        ('origin_city', pa.string()),
        ('origin_country', pa.string()),
        ('dest_code', pa.string()),
        ('dest_city', pa.string()),
        ('dest_country', pa.string()),
        ('valid_from', pa.date32()),
        ('valid_to', pa.date32()),
        ('days_mask', pa.string()),
        ('dep_time', pa.string()),
        ('arr_time', pa.string()),
        ('arr_day_offset', pa.int16()),
        ('travel_time_min', pa.int32()),
        ('aircraft_code', pa.string()),
        ('aircraft_name', pa.string()),
        ('source_file', pa.string())
    ])

    # Очистка от NaN
    df_final = df_final.astype('object').where(pd.notnull(df_final), None)
    table = pa.Table.from_pandas(df_final, schema=schema, preserve_index=False)

    # ЖЕСТКИЙ ФИКС ДАТ (чтобы не было миллисекунд)
    for col_idx, col_name in [(9, 'valid_from'), (10, 'valid_to')]:
        dates = []
        for x in df_final[col_name]:
            if x is None or pd.isna(x):
                dates.append(None)
            else:
                dates.append(pd.Timestamp(x).date())
        table = table.set_column(col_idx, col_name, pa.array(dates, type=pa.date32()))

    pq.write_table(table, parquet_output_path)

    print("\nГотово! Первые 5 строк справочника рейсов:")
    print(df_final[['schedule_id', 'flight_number', 'dest_city', 'valid_from', 'dep_time']].head())


if __name__ == '__main__':
    create_dim_flight_schedule(
        clean_input_path=r'BoardingData_clean.parquet',
        parquet_output_path=r'dim_flight_schedule.parquet'
    )