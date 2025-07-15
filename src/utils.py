import os
import shutil
import sqlite3

import pandas as pd
import requests


def get_data(db_url: str, file: str, backup_file: str, overwrite: bool = False) -> None:
    """
    Get data from Google API to use in the chatbot demo.

    Args:
        db_url: travel sql url path.
        file: database path.
        backup_file: backup database path.
    """

    if overwrite or os.path.exists(file):
        response = requests.get(url=db_url)
        response.raise_for_status()  # Raises an HTTPError, if one occurred.

        with open(file=file, mode='wb') as f:
            f.write(response.content)

        # Backup file. We will use this to "reset" the database in each section.
        shutil.copy(src=file, dst=backup_file)


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
