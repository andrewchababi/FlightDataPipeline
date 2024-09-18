from base_df_class import BaseDf

import datetime
import pandas as pd


class ViewDF(BaseDf):
    def __init__(self):
        super().__init__()
        self.columns_of_interest = ['flight', 'time', 'destination', 'gate']
        self.todays_date = datetime.date.today()
        self.process_data()


    def process_data(self):
        """Fetch flight data from the API and process it with the required filters and transformations."""
        try:
            self.df = self.filter_flights_by_gate_range(self.df)
            self.df = self.fix_time_columns(self.df)
            self.df = self.split_planned_column(self.df)
            self.df = self.filter_today_flights(self.df)
            self.df = self.filter_columns(self.df)
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

    def filter_columns(self, df):
        """
        Filter out unnecessary columns from the DataFrame.

        :param df: DataFrame - The DataFrame with all columns.
        :return: DataFrame - DataFrame with only the relevant columns.
        """
        return df[self.columns_of_interest]

def main():
    view_df = ViewDF()
    view_df.df.to_html('template/view_df-html.html')


if __name__ == '__main__':
    main()