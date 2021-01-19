from datetime import datetime

from flask import Flask, request, jsonify, Response

app = Flask(__name__)


readings = []


@app.route("/sensor-readings", methods=["POST"])
def add_reading():
    reading = request.json
    readings.append(reading)
    return Response(status=204)


@app.route("/sensor-readings", methods=["GET"])
def get_reading():
    since_arg = request.args.get("since")
    if since_arg is None:
        return jsonify(readings)
    since = datetime.fromisoformat(since_arg)
    result = list(filter(lambda reading: datetime.fromisoformat(reading['timestamp']) >= since, readings))
    return jsonify(result)


if __name__ == '__main__':
    Flask.run(app)
