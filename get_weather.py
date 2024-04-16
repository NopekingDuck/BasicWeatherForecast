import datetime
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry


def get_weather(coords):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"

    #prepare the parameters: coords + variables + date
    variables = {
        "hourly": ["temperature_2m", "weather_code", "wind_speed_10m"],
        "wind_speed_unit": "mph",
        "timezone": "GMT",
    }
    start_end = {
        "start_date": datetime.date.today(),
        "end_date": datetime.date.today()
    }
    params = coords | variables | start_end
    responses = openmeteo.weather_api(url, params=params)

    # Process hourly data. The order of variables needs to be the same as requested.
    response = responses[0]
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_weather_code = hourly.Variables(1).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(2).ValuesAsNumpy()

    # create dataframe of hourly data
    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    ), "temperature_2m": hourly_temperature_2m, "weather_code": hourly_weather_code,
        "wind_speed_10m": hourly_wind_speed_10m
    }
    hourly_dataframe = pd.DataFrame(data=hourly_data)

    # Extract rows that match the timestamp for a 3 hourly forecast
    three_hourly_dataframe = hourly_dataframe.iloc[::3, :]

    return three_hourly_dataframe
