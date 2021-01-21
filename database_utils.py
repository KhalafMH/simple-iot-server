from datetime import datetime, timezone

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

cassandra_auth_provider = PlainTextAuthProvider(username="sensor_app", password="Y6p4b152J2fZ")
cassandra_session = Cluster(auth_provider=cassandra_auth_provider).connect("sensor_app")


def get_year_month(dt):
    return dt.strftime("%Y-%m")


def get_year_months_since_timestamp(timestamp: datetime):
    year_months = []
    now = datetime.now().astimezone(timezone.utc)
    i = timestamp.astimezone(timezone.utc)
    while i.year < now.year or (i.year == now.year and i.month <= now.month):
        year_months.append(get_year_month(i))
        if i.month == 12:
            year = i.year + 1
            month = 1
        else:
            year = i.year
            month = i.month + 1
        i = datetime(year, month, i.day)
    return year_months


def reading_record_mapper(record):
    (id, type, value, alert, timestamp) = record
    return {"id": id, "type": type, "value": value, "alert": alert, "timestamp": timestamp.isoformat()}


def add_reading_to_database(reading):
    timestamp = datetime.fromisoformat(reading["timestamp"]).astimezone(timezone.utc)
    year_month = timestamp.strftime("%Y-%m")
    cassandra_session.execute(
        "INSERT INTO readings (id, year_month, timestamp, type, value, alert)"
        " values (%(id)s, %(year_month)s, %(timestamp)s, %(type)s, %(value)s, %(alert)s)",
        {
            "id": reading["id"],
            "year_month": year_month,
            "timestamp": timestamp,
            "type": reading["type"],
            "value": reading["value"],
            "alert": reading["alert"]
        }
    )


def get_readings_from_database(id=None, year_month=None):
    if year_month is None:
        year_month = get_year_month(datetime.now().astimezone(timezone.utc))
    if id is None:
        cursor = cassandra_session.execute(
            "SELECT id, type, value, alert, timestamp FROM readings WHERE year_month = %(year_month)s ALLOW FILTERING",
            {"year_month": year_month}
        )
        return list(map(reading_record_mapper, cursor))
    cursor = cassandra_session.execute(
        "SELECT id, type, value, alert, timestamp FROM readings WHERE id = %(id)s AND year_month = %(year_month)s",
        {
            "id": id,
            "year_month": year_month
        }
    )
    result = list(map(reading_record_mapper, cursor))
    result.sort(key=lambda x: x["timestamp"], reverse=True)
    return result


def get_all_readings_from_database_since_timestamp(timestamp: datetime, id=None):
    if id is None:
        cursor = cassandra_session.execute(
            "SELECT id, type, value, alert, timestamp FROM readings "
            "WHERE timestamp >= %(timestamp)s ALLOW FILTERING",
            {"timestamp": timestamp}
        )
        result = list(map(reading_record_mapper, cursor))
        result.sort(key=lambda x: x["timestamp"], reverse=True)
        return result
    else:
        year_months = get_year_months_since_timestamp(timestamp)
        year_months_repr = ','.join(map(lambda x: f"'{x}'", year_months))
        cursor = cassandra_session.execute(
            f"SELECT id, type, value, alert, timestamp FROM readings "
            f"WHERE id = %(id)s and year_month IN ({year_months_repr}) and timestamp >= %(timestamp)s",
            {
                "id": id,
                "timestamp": timestamp
            }
        )
        result = list(map(reading_record_mapper, cursor))
        result.sort(key=lambda x: x["timestamp"], reverse=True)
        return result
