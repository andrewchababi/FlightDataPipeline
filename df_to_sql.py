from flight_fetching import process_flights_to_df
from sqlalchemy import create_engine


def store_dataframe_to_mysql(df, table_name, connection_string):
    """
	Store a pandas DataFrame into a MySQL table.

    Args:
    df (pandas.DataFrame): The DataFrame to store.
    table_name (str): The name of the table to store the data.
    connection_string (str): The MySQL connection string.
    """
    engine = create_engine(connection_string)
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"Data stored in table '{table_name}' successfully.")


def main():
    """
	Main function to orchestrate the data fetching, parsing, and converting to DataFrame.
	"""
    # Define the URL for flight data
    url = 'https://www.admtl.com/en/admtldata/api/flight?type=departure&sort=field_planned&direction=ASC&rule=24h'

    flights_df = process_flights_to_df(url)

    # Store DataFrame to MySQL
    connection_string = 'mysql+pymysql://root:VavaChab!2!6@localhost:3306/flights_data'
    store_dataframe_to_mysql(flights_df, 'flights', connection_string)


if __name__ == "__main__":
    main()
