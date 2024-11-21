#Tharron's task
#Starter code

import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Base URL for the COVID-19 Statistics API
BASE_URL = "https://covid-api.com/api/reports"
# Note: This API does not require a key, but you might need one for other endpoints or APIs.

def fetch_covid_data(region, date=None):
    """
    Fetch COVID-19 data for a specific region (e.g., Michigan or National) using the COVID-19 Statistics API.

    Args:
        region (str): Name of the region (e.g., 'Michigan', 'US').
        date (str, optional): Specific date to fetch data for in YYYY-MM-DD format. Defaults to None (fetch latest data).

    Returns:
        dict: JSON response containing COVID-19 data, or None if the request fails.
    """
    # Parameters for the API request
    params = {
        'region_name': region,  # Name of the region
    }
    if date:
        # Include date in the request if specified
        params['date'] = date

    try:
        # Send GET request to the API
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors
        return response.json()  # Return JSON response
    except requests.exceptions.RequestException as e:
        # Print error message if the request fails
        print(f"Failed to fetch COVID-19 data for {region}: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Fetch COVID-19 data for Michigan
    region = "Michigan"
    date = "2023-01-01"  # Replace with desired date or omit for the latest data

    covid_data = fetch_covid_data(region, date)
    # Print fetched data for debugging purposes
    print(covid_data)

