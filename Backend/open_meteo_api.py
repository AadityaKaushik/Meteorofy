import requests
import pandas as pd

def get_latest_features(latitude, longitude):
    timezone = "Asia/Kolkata"

    # Fetch weather
    forecast_url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,windspeed_10m,windgusts_10m,relative_humidity_2m,pressure_msl,precipitation,soil_moisture_0_1cm",
        "timezone": timezone
    }
    weather_res = requests.get(forecast_url, params=params).json()
    hourly_df = pd.DataFrame(weather_res['hourly'])
    hourly_df['time'] = pd.to_datetime(hourly_df['time'])
    hourly_df['windspeed_10m'] = hourly_df['windspeed_10m'] * 3.6
    hourly_df['windgusts_10m'] = hourly_df['windgusts_10m'] * 3.6

    # Marine data
    marine_url = "https://api.open-meteo.com/v1/marine"
    marine_params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "wave_height,sea_surface_temperature",
        "timezone": timezone
    }
    marine_res = requests.get(marine_url, params=marine_params).json()
    marine_df = pd.DataFrame(marine_res.get('hourly', []))
    if not marine_df.empty:
        marine_df['time'] = pd.to_datetime(marine_df['time'])
    else:
        marine_df = pd.DataFrame(columns=['time','wave_height','sea_surface_temperature'])

    # Merge
    final_df = pd.merge(hourly_df, marine_df, on='time', how='outer')
    latest = final_df.sort_values('time').iloc[-1]

    # Rename columns
    latest = latest.rename({
        'temperature_2m': 'temperature_2m',
        'windspeed_10m': 'windspeed_10m',
        'windgusts_10m': 'windgusts_10m',
        'relative_humidity_2m': 'humidity_pct',
        'pressure_msl': 'pressure_hpa',
        'precipitation': 'precipitation_mm',
        'soil_moisture_0_1cm': 'soil_moisture',
        'wave_height': 'wave_height_m',
        'sea_surface_temperature': 'sea_temp_c'
    })

    # Fill NaNs
    if pd.isna(latest['wave_height_m']):
        latest['wave_height_m'] = 0
    if pd.isna(latest['sea_temp_c']):
        latest['sea_temp_c'] = latest['temperature_2m']

    # Drop the time column
    if 'time' in latest:
        latest = latest.drop(labels=['time'])

    return latest
