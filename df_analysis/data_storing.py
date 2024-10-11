import pandas as pd
import datetime
from sqlalchemy import create_engine

from view_df_sql import ViewDF

connection_string = 'mysql+pymysql://root:VavaChab!2!6@localhost:3306/flights_data'

todays_date = datetime.date.today()

columns_list = [
    "flight_date",
    "year",
    "month",
    "day",
    "day_of_week",
    "is_weekend",
    "total_num_flights",
    "delayed_flights",
    "6-12pm",
    "6-12am",
    "3-6am",
    "12-6pm",
    "gate_68",
    "gate_67",
    "gate_66",
    "gate_65",
    "gate_64",
    "gate_63",
    "gate_62C",
    "gate_62B",
    "gate_62A",
    "Toulouse",
    "San Salvador",
    "San Jose",
    "Samana",
    "Rome",
    "Punta Cana",
    "Paris  Ch.de Gaulle",
    "Panama/Panama City",
    "Marseille",
    "Lyon",
    "Istanbul",
    "Francfort/Frankfurt",
    "Dubai",
    "Doha",
    "Cayo Coco",
    "Casablanca",
    "Cancun",
    "Bogota",
    "Athenes/Athens",
    "Amman",
    "Alger/Algiers",
]


class Analysis:
    def __init__(self):
        self.analysis_df = pd.DataFrame(columns=columns_list)
        self.data = ViewDF().df
        self.analyse()

    def analyse(self):
        # Example row creation
        row_data = self.generate_row(self.data)
        self.analysis_df = pd.concat([self.analysis_df, row_data], ignore_index=True)

    def generate_row(self, df):
        # Convert the 'planned_time' column to datetime if it's not already
        df['planned_time'] = pd.to_datetime(df['planned_time'], errors='coerce')

        # Initialize the row with default values
        row_data = {col: 0 for col in columns_list}

        # Fill in specific values based on the data
        row_data['flight_date'] = todays_date
        row_data['year'] = todays_date.year
        row_data['month'] = todays_date.month
        row_data['day'] = todays_date.day
        row_data['day_of_week'] = todays_date.weekday()  # 0 = Monday, 6 = Sunday
        row_data['is_weekend'] = int(todays_date.weekday() >= 5)  # 1 for weekend, 0 for weekday
        row_data['total_num_flights'] = len(df)
        row_data['delayed_flights'] = len(df[df['planned_time'] != df['revised_time']])

        # Count flights in specific time slots
        row_data['6-12pm'] = len(df[(df['planned_time'].dt.hour >= 18) & (df['planned_time'].dt.hour < 24)])
        row_data['6-12am'] = len(df[(df['planned_time'].dt.hour >= 6) & (df['planned_time'].dt.hour < 12)])
        row_data['3-6am'] = len(df[(df['planned_time'].dt.hour >= 3) & (df['planned_time'].dt.hour < 6)])
        row_data['12-6pm'] = len(df[(df['planned_time'].dt.hour >= 12) & (df['planned_time'].dt.hour < 18)])

        # Count flights per gate
        for gate in ['68', '67', '66', '65', '64', '63', '62C', '62B', '62A']:
            row_data[f'gate_{gate}'] = len(df[df['gate'] == gate])

        # Count flights per destination
        destination_count = df['destination'].value_counts()
        for destination in destination_count.index:
            if destination in row_data:
                row_data[destination] = destination_count[destination]

        # Convert row_data to DataFrame and return
        return pd.DataFrame([row_data])

    def save_to_db(self, table_name, connection_string):
        """
        Store the DataFrame into a MySQL table.

        :param table_name: str - The name of the table to store the data.
        :param connection_string: str - The MySQL connection string.
        """
        engine = create_engine(connection_string)
        try:
            self.analysis_df.to_sql(table_name, con=engine, if_exists='append', index=False)
            print(f"Data stored in table '{table_name}' successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")


def main():
    # Set display options to show all columns
    pd.set_option('display.max_columns', None)  # Show all columns
    pd.set_option('display.expand_frame_repr', False)  # Don't wrap columns

    data = Analysis()
    print(data.analysis_df.head())
    data.save_to_db(table_name='data_set', connection_string=connection_string)


if __name__ == '__main__':
    main()
