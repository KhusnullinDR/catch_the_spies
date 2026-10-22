import duckdb

con = duckdb.connect()

con.execute("""
    COPY (
        SELECT
            flight_date,
            flight_number,
            airline_code,
            origin      AS origin_code,
            destination AS dest_code,
            status,
            ff_airline,
            ff_number,
            booking_class,
            fare_code,
            source_file
        FROM 'flight_events.parquet'
    )
    TO 'flight_events_v2.parquet'
    (FORMAT PARQUET, COMPRESSION 'snappy')
""")

print("done: out/flight_events_v2.parquet")