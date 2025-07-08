import pandas as pd
import yaml
import requests

def get_weather_data_df(base_url, data_key="hourly", export=False, filename=None, location_name=None, **params):
    """
    Fetches weather data from a given API and returns it as a pandas DataFrame.

    Args:
        base_url (str): The base URL of the weather API.
        data_key (str): The key to access weather data in the API response (e.g., "hourly").
        export (bool): Whether to export the resulting DataFrame to a CSV file.
        filename (str): Filename to use when exporting the DataFrame to CSV.
        location_name (str): Optional name of the location (used as a column).
        **params: Additional query parameters (e.g., latitude, longitude, hourly variables).

    Returns:
        pd.DataFrame: A DataFrame containing the weather data with time, units, and location details.
    """
    if isinstance(params.get("hourly"), list):
        params["hourly"] = ",".join(params["hourly"])

    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()

    if data_key not in data:
        raise KeyError(f"'{data_key}' not found in the response")

    weather_data = data[data_key]
    units = data.get(f"{data_key}_units", {})
    timezone = data.get("timezone", "UTC")

    time_col_name = f"time ({timezone})" if timezone else "time"
    timestamps = weather_data.get("time", [])

    rows = []
    for i in range(len(timestamps)):
        row = {time_col_name: timestamps[i]}
        for key in weather_data:
            if key != "time":
                unit = units.get(key, "")
                label = f"{key} ({unit})" if unit else key
                row[label] = weather_data[key][i]
        rows.append(row)

    df = pd.DataFrame(rows)

    lat = data.get("latitude", params.get("latitude"))
    lon = data.get("longitude", params.get("longitude"))
    ele = data.get("elevation", params.get("elevation"))

    df.insert(0, "latitude", lat)
    df.insert(1, "longitude", lon)
    df.insert(2, "elevation", ele)

    time_col = [col for col in df.columns if col.startswith("time")][0]
    time_data = df.pop(time_col)
    df.insert(3, time_col, time_data)

    if location_name:
        df.insert(0, "location", location_name)

    if export and filename:
        df.to_csv(filename, index=False, encoding='utf-8')

    return df

def get_weather_data_multiple_locations(base_url, locations, data_key="hourly", export=False, **params):
    """
    Fetches weather data for multiple locations and returns a list of DataFrames.

    Args:
        base_url (str): The base URL of the weather API.
        locations (dict): A dictionary of location names mapped to (latitude, longitude) tuples.
        data_key (str): The key to access weather data in the API response.
        export (bool): Whether to export each location's data to a file (unused here).
        **params: Additional query parameters shared across all locations.

    Returns:
        List[pd.DataFrame]: A list of DataFrames for each location's weather data.
    """
    all_dfs = []

    for name, (lat, lon) in locations.items():
        print(f"Fetching data for {name}: ({lat}, {lon})")
        df = get_weather_data_df(
            base_url,
            data_key=data_key,
            export=False,
            latitude=lat,
            longitude=lon,
            location_name=name,
            **params
        )
        all_dfs.append(df)

    return all_dfs

if __name__ == "__main__":
    """
    Main execution block: Loads default configuration, fetches weather data
    for multiple predefined locations, combines them, and saves to a CSV.
    """
    with open("defaults.yaml", "r") as f:
        default_config = yaml.safe_load(f)

    base_url = default_config.pop("base_url")

    locations = {
        "Nashik": (20.5943, 74.3715),
        "Pune": (18.5204, 73.8567),
        "Mumbai": (19.0760, 72.8777),
    }

    dfs = get_weather_data_multiple_locations(
        base_url,
        locations,
        data_key="hourly",
        export=False,
        **default_config
    )

    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df.to_csv("abc1.csv", index=False, encoding='utf-8')
    print("Saved complete data to abc1.csv")
    print("Data fetched and saved successfully.")
