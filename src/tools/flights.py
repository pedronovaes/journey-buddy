"""
Assistant's tools for flights topics.
"""

import sqlite3
from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig


@tool
def fetch_user_flight_information(config: RunnableConfig) -> list[dict]:
    """
    Fetch all tickets for a user along with corresponding flight information
    and seat assignments.

    Returns:
        A list of dictionaries where each dictionary contains the ticket
        details, associated flight details, and the seat assignments for each
        ticket belonging to the user.
    """

    configuration = config.get('configurable', {})
    passenger_id = configuration.get('passenger_id', {})
    db = configuration.get('db', '')

    if not passenger_id:
        raise ValueError('No passenger ID configured.')

    conn = sqlite3.connect(database=db)
    cursor = conn.cursor()

    query = """
    select
        t.ticket_no, t.book_ref, f.flight_id, f.flight_no, f.departure_airport,
        f.arrival_airport, f.scheduled_departure, f.scheduled_arrival,
        bp.seat_no, tf.fare_conditions
    from
        tickets t
        join ticket_flights tf on t.ticket_no = tf.ticket_no
        join flights f on tf.flight_id = f.flight_id
        join boarding_passes bp on bp.ticket_no = t.ticket_no
            and bp.flight_id = f.flight_id
    where
        t.passenger_id = ?
    """

    cursor.execute(query, (passenger_id,))
    rows = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    results = [dict(zip(column_names, row)) for row in rows]

    cursor.close()
    conn.close()

    return results
