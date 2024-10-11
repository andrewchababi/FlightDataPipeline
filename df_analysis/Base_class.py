import json
import pandas as pd
import requests
from sqlalchemy import create_engine


class BaseDf:

    def __init__(self):
        """
        Initialize the processor with the URL and relevant columns.

        param url: str - The URL to fetch flight data.
        param columns_of_interest: list - The columns to retain in the final DataFrame.
        """
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
        self.store_to_db('flights', connection_string='mysql+pymysql://root:VavaChab!2!6@localhost:3306/flights_data')

    def store_to_db(self, table_name, connection_string):
        """
        Store the DataFrame into a MySQL table.

        :param table_name: str - The name of the table to store the data.
        :param connection_string: str - The MySQL connection string.
        """
        engine = create_engine(connection_string)
        try:
            self.df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            print(f"Data stored in table '{table_name}' successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")


def main():
    raw_df = BaseDf()
    raw_df.df.to_html('template/Base_df-html.html')


if __name__ == '__main__':
    main()
