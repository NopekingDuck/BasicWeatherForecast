from flask import Flask, request, jsonify, render_template
from markupsafe import escape#
import get_weather
import helpers

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        coords = {
                "latitude": request.form.get("latitude"),
                "longitude": request.form.get("longitude")
        }
        dataframe = get_weather.get_weather(coords)
        out_df = helpers.process_df(dataframe)
        return render_template('response.html', name=out_df)
    return render_template("index.html")





