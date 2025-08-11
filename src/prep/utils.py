"""
Utils functions to handle chatbot data.
"""

import os
import shutil
import sqlite3

import pandas as pd
import requests


def get_data(file: str, backup_file: str, overwrite: bool = False) -> None:
    """
    Fetch a SQLite database from Google API data to use on the chatbot.

    Args:
        file: database path.
        backup_file: backup database path.
        overwrite: boolean to indicate if the database will be overwrited, if
            exists.
    """

    db_url = 'https://storage.googleapis.com/benchmarks-artifacts/travel-db/travel2.sqlite'

    if overwrite or not os.path.exists(file):
        response = requests.get(url=db_url)
        response.raise_for_status()  # Raises an HTTPError, if one occurred.

        with open(file=file, mode='wb') as f:
            f.write(response.content)

        # Backup file. We will use this to "reset" the db in each section the
        # chatbot will be executed.
        shutil.copy(src=file, dst=backup_file)

        print('Flight data downloaded with success.')


def update_dates(file: str, backup_file: str) -> str:
    """
    Convert the flights to current time during runtime.

    Args:
        file: database path.
        backup_file: backup database path.
    """

    # Using original database file (that is stored in the backup file).
    shutil.copy(src=backup_file, dst=file)

    conn = sqlite3.connect(database=file)

    # Get tables names.
    tables = (
        pd.read_sql(sql="select name from sqlite_master where type = 'table'", con=conn)
        .name
        .to_list()
    )

    # Convert each table to pandas dataframe.
    tdf = {}
    for t in tables:
        tdf[t] = pd.read_sql(sql=f"select * from {t}", con=conn)

    example_time = (
        pd.to_datetime(tdf['flights']['actual_departure'])
        .max()
    )
    current_time = pd.to_datetime('now').tz_localize(example_time.tz)
    time_diff = current_time - example_time

    # Convert timestamps to current time (only in "flights" and "bookings"
    # tables).
    tdf['bookings']['book_date'] = (
        pd.to_datetime(tdf['bookings']['book_date'], utc=True) + time_diff
    )

    datetime_columns = [
        'scheduled_departure',
        'scheduled_arrival',
        'actual_departure',
        'actual_arrival'
    ]
    for column in datetime_columns:
        tdf['flights'][column] = pd.to_datetime(tdf['flights'][column]) + time_diff

    # Save updated sqlite tables.
    for table_name, df in tdf.items():
        df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)

    conn.commit()
    conn.close()

    return file
