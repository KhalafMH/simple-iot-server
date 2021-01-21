from flask import Flask, request, jsonify, Response

from database_utils import *

app = Flask(__name__)


@app.route("/sensor-readings", methods=["POST"])
def add_reading():
    reading = request.json
    add_reading_to_database(reading)
    return Response(status=204)


@app.route("/sensor-readings", methods=["GET"])
def get_readings():
    since_arg = request.args.get("since")
    sensor_id = request.args.get("sensor_id")
    if since_arg is None:
        return jsonify(get_readings_from_database(id=sensor_id))
    since = iso8601.parse_date(since_arg).astimezone(timezone.utc)
    result = get_all_readings_from_database_since_timestamp(timestamp=since, id=sensor_id)
    return jsonify(result)


if __name__ == '__main__':
    Flask.run(app, port="8080")
