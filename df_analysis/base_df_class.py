import json
import pandas as pd
import requests


class BaseDf:

    def __init__(self):
        """
        Initialize the processor with the URL and relevant columns.

        param url: str - The URL to fetch flight data.
        param columns_of_interest: list - The columns to retain in the final DataFrame.
        """
        self.columns_of_interest = None
        self.url = 'https://www.admtl.com/en/admtldata/api/flight?type=departure&sort=field_planned&direction=ASC&rule=24h'
        self.df = pd.DataFrame()
        self.raw_data = None
        self.structured_data = None
        self.initialise_base_df()

    def fetch_flight_data(self):
        """
        Fetch flight data from the provided URL.

        Returns:
        None - Assigns fetched data to raw_data attribute.
        """
        try:
            response = requests.get(self.url)
            response.raise_for_status()  # Raise an exception for HTTP errors
            self.raw_data = response.content
            print(f"HTTP Status Code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data: {e}")
            raise

    def parse_json_content(self):
        """
        Parse JSON content from raw_data and store in structured_data.

        Returns:
        None - Assigns parsed JSON to structured_data attribute.
        """
        if self.raw_data:
            self.structured_data = json.loads(self.raw_data)
        else:
            raise ValueError("Raw data not available. Fetch data first.")

    def convert_to_dataframe(self, key='data'):
        """
        Convert structured JSON data into a pandas DataFrame and store in df attribute.

        param key: str - Key in the JSON data that contains the list of records.
        """
        if self.structured_data:
            self.df = pd.json_normalize(self.structured_data[key])
        else:
            raise ValueError("Structured data not available. Parse data first.")

    def initialise_base_df(self):
        """
        Pipeline to fetch, parse, and convert flight data to a DataFrame.
        """
        self.fetch_flight_data()
        self.parse_json_content()
        self.convert_to_dataframe()


def main():
    raw_df = BaseDf()
    print(raw_df.df.columns)


if __name__ == '__main__':
    main()
