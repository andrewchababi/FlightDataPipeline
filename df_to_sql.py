from flight_fetching import *
from sqlalchemy import create_engine
import pymysql


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

	# Fetch the flight data
	response = fetch_flight_data(url)
	print(f"HTTP Status Code: {response.status_code}")

	# Step 1: Raw Data
	step_separator("FIRST STEP RAW DATA")
	raw_data = response.content
	print(raw_data)

	# Step 2: Structured Data
	step_separator("SECOND STEP STRUCTURED DATA")
	structured_data = parse_json_content(raw_data)
	print(format_json_data(structured_data))

	# Step 3: JSON Format Data
	step_separator("THIRD STEP JSON FORMAT DATA")
	flights_df = convert_to_dataframe(structured_data)
	print(flights_df.head())

	# Display columns of interest
	columns_of_interest = ['id', 'type', 'flight', 'planned', 'revised', 'company', 'compagny_without_accent',
						   'destination', 'gate']
	print(flights_df[columns_of_interest].head())

	# Store DataFrame to MySQL
	connection_string = 'mysql+pymysql://root:VavaChab!2!6@localhost:3306/flights_data'
	store_dataframe_to_mysql(flights_df, 'flights', connection_string)


if __name__ == "__main__":
	main()
