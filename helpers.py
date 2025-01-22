import json
from json import JSONDecodeError


def process_df(dataframe):
    out_df = dataframe
    # get dictionary of weather codes
    try:
        with open("static/assets/json/weather_codes.json") as wc_json:
            weather_codes = json.load(wc_json)
    except JSONDecodeError as e:
        raise Exception("Unable to get API variables from weather codes json: {e}")

    #   make a new column in the dataframe containing the weather string matching provided weather code in that row
    weather = []
    for value in dataframe['weather_code']:
        x = str(int(value))
        weather.append(weather_codes[x])
    out_df['weather'] = weather

    # remove the unneeded weather codes column
    out_df = out_df.drop('weather_code', axis=1)

    # round temperature to 1 digit and convert to int
    out_df['temperature_2m'] = dataframe['temperature_2m'].round(1)
    out_df['temperature_2m'] = out_df['temperature_2m'].astype(int)

    # round windspeed to 1 digit and convert to int
    out_df['wind_speed_10m'] = dataframe['wind_speed_10m'].round(1)
    out_df['wind_speed_10m'] = out_df['wind_speed_10m'].astype(int)

    return out_df
