# Starter Code
import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Base URL for NOAA Climate Data Online API
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
# API token for authentication (stored in .env for security)
TOKEN = os.getenv("NOAA_CDO_TOKEN")

def fetch_climate_data(station_id, start_date, end_date):
    """
    Fetch climate data (e.g., temperature) for a specific station and date range using NOAA's API.

    Args:
        station_id (str): Station ID for the location of interest.
        start_date (str): Start date in YYYY-MM-DD format.
        end_date (str): End date in YYYY-MM-DD format.

    Returns:
        dict: JSON response containing climate data, or None if the request fails.
    """
    # Headers for the API request (contains the API token)
    headers = {'token': TOKEN}
    # Query parameters for the request
    params = {
        'datasetid': 'GHCND',  # Dataset ID for daily summaries
        'stationid': station_id,  # Location-specific station ID
        'startdate': start_date,  # Start of the data range
        'enddate': end_date,  # End of the data range
        'datatypeid': ['TMAX', 'TMIN'],  # Fetch max and min temperatures
        'limit': 1000  # Max number of results per request
    }

    try:
        # Send GET request to the API
        response = requests.get(BASE_URL, headers=headers, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors
        return response.json()  # Return JSON response
    except requests.exceptions.RequestException as e:
        # Print error message if the request fails
        print(f"Error fetching climate data: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Example parameters for a specific station and date range
    station_id = "GHCND:USW00094847"  # Example station ID
    start_date = "2022-01-01"
    end_date = "2022-01-07"

    # Fetch climate data for the given parameters
    climate_data = fetch_climate_data(station_id, start_date, end_date)
    # Print fetched data for debugging purposes
    print(climate_data)

