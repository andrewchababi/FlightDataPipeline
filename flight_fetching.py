import requests
import json
import pandas as pd


SEPARATOR = """
=========================================================================================================
=========================================================================================================
"""


def step_separator(step_description):
    """
    Print a separator with the step description for better readability.

    Args:
    step_description (str): Description of the current step.
    """
    print(f"{SEPARATOR}\n{step_description}\n{SEPARATOR}")


def fetch_flight_data(url):
    """
    Fetch flight data from the given URL.

    Args:
    url (str): The URL to fetch the flight data from.

    Returns:
    response (requests.Response): The response object containing the flight data.
    """
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response


def parse_json_content(response_content):
    """
    Parse JSON content from the response content.

    Args:
    response_content (bytes): The response content in bytes.

    Returns:
    dict: The parsed JSON content as a dictionary.
    """
    return json.loads(response_content)


def format_json_data(json_data):
    """
    Format JSON data with indentation for better readability.

    Args:
    json_data (dict): The JSON data to format.

    Returns:
    str: The formatted JSON data as a string.
    """
    return json.dumps(json_data, indent=4)


def convert_to_dataframe(json_data, key='data'):
    """
    Convert JSON data to a pandas DataFrame.

    Args:
    json_data (dict): The JSON data to convert.
    key (str): The key in the JSON data that contains the list of records.

    Returns:
    pandas.DataFrame: The resulting DataFrame.
    """
    return pd.json_normalize(json_data[key])


def process_flights_to_df(url):
    # Fetch the flight data
    response = fetch_flight_data(url)
    print(f"HTTP Status Code: {response.status_code}")

    # Step 1: Raw Data
    #step_separator("FIRST STEP RAW DATA")
    raw_data = response.content
    #print(raw_data)

    # Step 2: Structured Data
    #step_separator("SECOND STEP STRUCTURED DATA")
    structured_data = parse_json_content(raw_data)
    #print(structured_data)

    # Step 3: JSON Format Data
    #step_separator("THIRD STEP JSON FORMAT DATA")
    flights_df = convert_to_dataframe(structured_data)
    #print(flights_df.head())

    # Display columns of interest
    columns_of_interest = ['id', 'type', 'flight', 'planned', 'revised', 'company', 'compagny_without_accent', 'destination', 'gate']
    #print(flights_df[columns_of_interest].head())

    # Return the DataFrame for further use
    return flights_df
