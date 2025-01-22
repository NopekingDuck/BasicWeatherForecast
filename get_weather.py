import datetime
import json
import urllib3
import pandas as pd
from json import JSONDecodeError
from urllib.parse import urlencode
from urllib3.util import Timeout
from urllib3 import Retry


def make_url(coords, variables):
    url = "https://api.open-meteo.com/v1/forecast"

    start_end = {
        "start_date": datetime.date.today(),
        "end_date": datetime.date.today()
    }
    params = coords | variables | start_end
    encoded_params = urlencode(params, doseq=True)
    full_url = url + "?" + encoded_params
    return full_url


def get_data_from_api(url):
    retries = Retry(
        total=5,
        backoff_factor=0.2,
        status_forcelist=[500, 502, 503, 504]
    )

    http = urllib3.PoolManager(retries=retries)

    try:
        response = http.request("GET", url, timeout=Timeout(connect=1.0, read=2.0))
        if response.status >= 400:
            raise Exception(f"Error connecting to api. HTTP error status code: {response.status}")
        else:
            print("Request successful")
            data = response.data
            values = json.loads(data)
            return values
    except urllib3.exceptions.MaxRetryError as e:
        raise Exception(f"Max retries exceeded with url: {e.reason}")
    except urllib3.exceptions.TimeoutError as e:
        raise Exception(f"Request timed out: {e}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {e}")


def response_to_pandas(response, weather_types):
    try:
        hourly = response["hourly"]
    except TypeError as e:
        raise Exception(f"No response received from Weather source: {e}")
    date_column = {
        "date": pd.date_range(
                start=pd.to_datetime(hourly["time"][0]),
                end=pd.to_datetime(hourly["time"][len(hourly["time"]) - 1]),
                freq="h",
            )
    }

    optional_weather = {}
    for wt in weather_types["hourly"]:
        optional_weather[wt] = hourly[wt]

    hourly_data = date_column | optional_weather
    hourly_dataframe = pd.DataFrame(data=hourly_data)
    three_hourly_dataframe = hourly_dataframe.iloc[::3, :]
    return three_hourly_dataframe

def get_weather(coords):
    try:
        with open("static/assets/json/variables.json") as variables_json:
            variables = json.load(variables_json)
    except JSONDecodeError as e:
        raise Exception(f"Unable to get API variables from variables json: {e}")
    url = make_url(coords, variables)
    response = get_data_from_api(url)
    df = response_to_pandas(response, variables)
    return df
