from flight_fetching import *
from sql_to_html import save_dataframe_to_html
import datetime

todays_date = datetime.date.today()

url = 'https://www.admtl.com/en/admtldata/api/flight?type=departure&sort=field_planned&direction=ASC&rule=24h'

columns_of_interest = ['id', 'flight', 'planned', 'revised', 'destination', 'gate']



def filter_flights_by_gate_range(raw_df, min_gate=62, max_gate=68):
    # Convert 'gate' column to numeric, coercing errors to NaN, then downcast to integer
    raw_df['gate'] = pd.to_numeric(raw_df['gate'], downcast='integer', errors='coerce')

    # Drop rows where 'gate' is NaN
    raw_df = raw_df.dropna(subset=['gate'])

    # Ensure the 'gate' column is converted to integers
    raw_df['gate'] = raw_df['gate'].astype(int)

    # Filter the DataFrame for gate numbers within the specified range
    relevant_flights = raw_df.loc[(raw_df['gate'] >= min_gate) & (raw_df['gate'] <= max_gate)]

    relevant_flights = relevant_flights.reset_index(drop=True)

    return relevant_flights


def filter_df_columns(filtered_df):
    for column in filtered_df.columns:
        if column not in columns_of_interest:
            filtered_df = filtered_df.drop(column, axis=1)
    return filtered_df


def fix_time_columnn(df):
    df['planned'] = pd.to_datetime(df['planned'], unit='s') - pd.Timedelta(hours=4)
    df['revised'] = pd.to_datetime(df['revised'], unit='s') - pd.Timedelta(hours=4)
    return df


def main():
    raw_df = process_flights_to_df(url)

    filtered_df = filter_flights_by_gate_range(raw_df)

    filtered_column_df = filter_df_columns(filtered_df)

    fix_time_columnn(filtered_column_df)
    filtered_column_df.rename(columns={'compagny_without_accent': 'company'}, inplace=True)
    save_dataframe_to_html(filtered_column_df, 'templates/df-data.html')


if __name__ == '__main__':
    main()
