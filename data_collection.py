from datetime import datetime
import pandas as pd
import sqlite3
import requests
from meteostat import Point, Daily
from epiweeks import Week

# Constants
DB_NAME = "final_project.db"
START_DATE = datetime(2020, 3, 1)
END_DATE = datetime(2023, 3, 1)
COVID_API_KEY = "a8f75039e13840afadef9ba92b998956"
COVID_BASE_URL = "https://api.covidactnow.org/v2"
FLU_API_KEY = "8120b330a58f8"

# Location Constants
MICHIGAN_LOCATION = Point(42.3314, -83.0458)  # Detroit, Michigan
NATIONAL_LOCATIONS = {
    "New York": Point(40.7128, -74.0060),
    "Los Angeles": Point(34.0522, -118.2437),
    "Chicago": Point(41.8781, -87.6298),
    "Houston": Point(29.7604, -95.3698),
    "Miami": Point(25.7617, -80.1918)
}

def get_db_connection():
    return sqlite3.connect(DB_NAME)

def get_week_id(date):
    parsed_date = datetime.strptime(date, '%Y-%m-%d')
    return int(parsed_date.strftime('%Y%U'))

def process_weather_data(location, start_date, end_date, table_name):
    data = Daily(location, start_date, end_date).fetch()
    data.index = data.index.to_series().apply(pd.to_datetime)
    
    conn = get_db_connection()
    data.reset_index(inplace=True)
    data.to_sql("weather_data", conn, if_exists="append", index=False)
    
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" AS
    SELECT
        CAST(strftime('%Y', time) AS INTEGER) * 100 + CAST(strftime('%W', time) AS INTEGER) AS week_id,
        AVG((tavg * 9/5) + 32) AS tavg_f,
        AVG((tmin * 9/5) + 32) AS tmin_f,
        AVG((tmax * 9/5) + 32) AS tmax_f
    FROM weather_data
    GROUP BY week_id;
    """)
    conn.commit()
    conn.close()

def process_all_cities():
    for city, location in NATIONAL_LOCATIONS.items():
        process_weather_data(location, START_DATE, END_DATE, f"{city.lower().replace(' ', '_')}_weather")

def calculate_national_weather_average():
    conn = get_db_connection()
    national_query = """
    CREATE TABLE IF NOT EXISTS national_weather AS
    SELECT
        week_id,
        AVG(tavg_f) AS tavg_f,
        AVG(tmin_f) AS tmin_f,
        AVG(tmax_f) AS tmax_f
    FROM (
        SELECT * FROM "new_york_weather"
        UNION ALL
        SELECT * FROM "los_angeles_weather"
        UNION ALL
        SELECT * FROM "chicago_weather"
        UNION ALL
        SELECT * FROM "houston_weather"
        UNION ALL
        SELECT * FROM "miami_weather"
    )
    GROUP BY week_id;
    """
    conn.execute(national_query)
    conn.commit()
    conn.close()

def fetch_and_store_michigan_covid():
    url = f"{COVID_BASE_URL}/state/MI.timeseries.json?apiKey={COVID_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        store_covid_data(response.json(), "michigan_covid_data")

def fetch_and_store_national_covid():
    url = f"{COVID_BASE_URL}/country/US.timeseries.json?apiKey={COVID_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        store_covid_data(response.json(), "national_covid_data")

def store_covid_data(data, table_name):
    conn = get_db_connection()
    timeseries = data.get("actualsTimeseries", [])
    df = pd.DataFrame(timeseries)
    
    for col in df.columns:
        if isinstance(df[col].iloc[0], (dict, list)):
            df[col] = df[col].apply(str)
    
    df['date'] = pd.to_datetime(df['date'])
    df.to_sql("raw_covid_data", conn, if_exists="append", index=False)
    
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" AS
    WITH daily_cases AS (
        SELECT
            date,
            cases - LAG(cases, 1) OVER (ORDER BY date) AS daily_cases
        FROM raw_covid_data
        WHERE cases IS NOT NULL
    )
    SELECT
        CAST(strftime('%Y', date) AS INTEGER) * 100 + CAST(strftime('%W', date) AS INTEGER) AS week_id,
        SUM(daily_cases) AS weekly_cases
    FROM daily_cases
    WHERE daily_cases IS NOT NULL
    GROUP BY week_id;
    """)
    conn.commit()
    conn.close()

def fetch_and_store_flu_data():
    regions = ["mi", "nat"]
    epiweeks = "202010-202310"
    base_url = "https://api.delphi.cmu.edu/epidata/fluview/"
    params = {"regions": ",".join(regions), "epiweeks": epiweeks, "auth": FLU_API_KEY}
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data["result"] == 1:
            df = pd.DataFrame(data["epidata"])
            df["date"] = df["epiweek"].apply(
                lambda ew: Week(int(str(ew)[:4]), int(str(ew)[4:])).startdate().strftime('%Y-%m-%d')
            )
            df["week_id"] = df["date"].apply(lambda d: get_week_id(d))
            
            conn = get_db_connection()
            df[["region", "week_id", "num_ili"]].to_sql(
                "flu_data_march_2020_to_2023_region_date_ili",
                conn,
                if_exists="replace",
                index=False
            )
            conn.close()
            print("Flu data stored successfully")
        else:
            print(f"API Error: {data['message']}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

def collect_all_data():
    # Process weather data
    process_weather_data(MICHIGAN_LOCATION, START_DATE, END_DATE, "michigan_weather")
    process_all_cities()
    calculate_national_weather_average()
    
    # Collect COVID data
    fetch_and_store_michigan_covid()
    fetch_and_store_national_covid()
    
    # Collect flu data
    fetch_and_store_flu_data()