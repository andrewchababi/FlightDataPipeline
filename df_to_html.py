import datetime
import pandas as pd
from flight_fetching import process_flights_to_df
from sql_to_html import save_dataframe_to_html


class FlightDataProcessor:
    """
    A class to handle processing of flight data.
    This includes filtering, transforming, and exporting the data.
    """

    def __init__(self, url, columns_of_interest, order):
        """
        Initialize the processor with the URL and relevant columns.

        :param url: str - The URL to fetch flight data.
        :param columns_of_interest: list - The columns to retain in the final DataFrame.
        """
        self.url = url
        self.columns_of_interest = columns_of_interest
        self.todays_date = datetime.date.today()
        self.df = pd.DataFrame()
        self.order = order

    def fetch_and_process_data(self):
        """Fetch flight data from the API and process it with the required filters and transformations."""
        try:
            self.df = process_flights_to_df(self.url)
            self.df = self.filter_flights_by_gate_range(self.df)
            self.df = self.filter_columns(self.df)
            self.df = self.fix_time_columns(self.df)
            self.df = self.split_planned_column(self.df)
            self.df = self.filter_today_flights(self.df)
            self.df = self.reorder_columns(self.df, self.order)

        except Exception as e:
            print(f"An error occurred while processing the data: {e}")
            raise

    def filter_flights_by_gate_range(self, df, min_gate=62, max_gate=68):
        """
        Filter flights by gate range.

        :param df: DataFrame - The raw DataFrame containing flight data.
        :param min_gate: int - Minimum gate number to filter by.
        :param max_gate: int - Maximum gate number to filter by.
        :return: DataFrame - Filtered DataFrame.
        """
        df['gate'] = pd.to_numeric(df['gate'], downcast='integer', errors='coerce')
        df = df.dropna(subset=['gate'])
        df['gate'] = df['gate'].astype(int)
        return df[(df['gate'] >= min_gate) & (df['gate'] <= max_gate)].reset_index(drop=True)

    def filter_columns(self, df):
        """
        Filter out unnecessary columns from the DataFrame.

        :param df: DataFrame - The DataFrame with all columns.
        :return: DataFrame - DataFrame with only the relevant columns.
        """
        return df[self.columns_of_interest]

    def fix_time_columns(self, df):
        """
        Convert and adjust time columns for the DataFrame.

        :param df: DataFrame - DataFrame with raw time columns.
        :return: DataFrame - DataFrame with fixed time columns.
        """
        df['planned'] = pd.to_datetime(df['planned'], unit='s') - pd.Timedelta(hours=4)
        df['revised'] = pd.to_datetime(df['revised'], unit='s') - pd.Timedelta(hours=4)
        return df

    def split_planned_column(self, df):
        """
        Split the 'planned' column into 'date' and 'time' columns.

        :param df: DataFrame - DataFrame with a 'planned' column.
        :return: DataFrame - DataFrame with separate 'date' and 'time' columns.
        """
        df['date'] = df['planned'].dt.date
        df['time'] = df['planned'].dt.time
        return df

    def filter_today_flights(self, df):
        """
        Filter out flights that are not scheduled for today.

        :param df: DataFrame - The DataFrame with flight data.
        :return: DataFrame - Filtered DataFrame with only today's flights.
        """
        return df[df['date'] == self.todays_date]

    def rename_columns(self, df):
        """
        Rename specific columns in the DataFrame.

        :param df: DataFrame - The DataFrame with columns to rename.
        :return: DataFrame - DataFrame with renamed columns.
        """
        return df.rename(columns={'compagny_without_accent': 'company'})

    def reorder_columns(self, df, order):
        """
        reorders the columns of the df according to the display needs

        :param df: A Pandas DataFrame
        :param order: A list with the name of the columns you would like to order them in
        :return: The ordered Df
        """
        return df[order]

    def export_to_html(self, output_path):
        """Save the processed DataFrame as an HTML file."""
        try:
            save_dataframe_to_html(self.df, output_path)
        except Exception as e:
            print(f"An error occurred while saving the DataFrame to HTML: {e}")
            raise


def main():
    url = 'https://www.admtl.com/en/admtldata/api/flight?type=departure&sort=field_planned&direction=ASC&rule=24h'
    columns_of_interest = ['id', 'flight', 'planned', 'revised', 'destination', 'gate']
    ordered_columns = ['id', 'flight', 'time', 'destination', 'gate']

    processor = FlightDataProcessor(url, columns_of_interest, ordered_columns)

    # Fetch and process flight data
    processor.fetch_and_process_data()

    # Rename columns and export to HTML
    processor.df = processor.rename_columns(processor.df)

    processor.export_to_html('templates/df-data.html')


if __name__ == '__main__':
    main()
