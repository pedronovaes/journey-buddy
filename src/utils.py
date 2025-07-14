import shutil
import sqlite3
import pandas as pd


def update_dates(file: str, backup_file: str) -> None:
    """
    Convert the flights to present time for the execution time.

    Args:
        file: database path.
        backup_file: backup database path.
    """

    shutil.copy(src=backup_file, dst=file)

    # SQLite connection.
    conn = sqlite3.connect(file)

    # Convert each table to pandas dataframe.
    tables = pd.read_sql(
        sql="select name from sqlite_master where type = 'table';",
        con=conn
    ).name.to_list()

    tdf = {}

    for t in tables:
        tdf[t] = pd.read_sql(sql=f'select * from {t}', con=conn)

    example_time = pd.to_datetime(
        tdf['flights']['actual_departure'].replace('\\N', pd.NaT)
    ).max()

    current_time = pd.to_datetime('now').tz_localize(example_time.tz)
    time_diff = current_time - example_time

    print(tdf.keys())

    # Updates datetime to look like it's current.
    tdf['bookings']['book_date'] = (
        pd.to_datetime(
            tdf['bookings']['book_date'].replace('\\N', pd.NaT),
            utc=True
        ) + time_diff
    )

    datetime_columns = [
        'scheduled_departure',
        'scheduled_arrival',
        'actual_departure',
        'actual_arrival'
    ]

    for column in datetime_columns:
        tdf['flights'][column] = (
            pd.to_datetime(
                tdf['flights'][column].replace('\\N', pd.NaT),
                utc=True
            ) + time_diff
        )

    # Save updated sqlite tables.
    for table_name, df in tdf.items():
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists='replace',
            index=False
        )

    conn.commit()
    conn.close()
