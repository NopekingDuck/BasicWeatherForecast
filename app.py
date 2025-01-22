from json import JSONDecodeError

from flask import Flask, request, render_template
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
        try:
            dataframe = get_weather.get_weather(coords)
            out_df = helpers.process_df(dataframe)
        except Exception as e:
            print(e)
            return render_template('error.html', error=e)
        else:
            return render_template('response.html', name=out_df)

    return render_template("index.html")





