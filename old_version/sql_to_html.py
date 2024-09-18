from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import date

import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection string
DATABASE_URI = 'mysql+pymysql://root:VavaChab!2!6@localhost:3306/flights_data'

# Today's date
todays_date = date.today().strftime('%Y-%m-%d')

# SQL query to fetch data from a specific table
QUERY = f"""
SELECT flight, 
       DATE_FORMAT(FROM_UNIXTIME(planned), '%%Y-%%m-%%d %%H:%%i') AS planned_time, 
       destination,
       gate, 
       company, 
       DATE_FORMAT(FROM_UNIXTIME(revised), '%%Y-%%m-%%d %%H:%%i') AS revised_time
FROM flights
WHERE CAST(gate AS UNSIGNED) BETWEEN 62 AND 68
  AND DATE(FROM_UNIXTIME(planned)) = '{todays_date}'
ORDER BY planned_time;"""


def create_engine_connection(uri):
    """
    Create a SQLAlchemy engine connection.

    Args:
        uri (str): The database URI.

    Returns:
        engine: The SQLAlchemy engine.
    """
    try:
        engine = create_engine(uri)
        logging.info("Database engine created successfully.")
        return engine
    except SQLAlchemyError as e:
        logging.error(f"Error creating database engine: {e}")
        raise


def fetch_data_to_dataframe(engine, query):
    """
    Fetch data from the database into a pandas DataFrame.

    Args:
        engine: The SQLAlchemy engine.
        query: The SQL query to execute.

    Returns:
        DataFrame: The resulting DataFrame.
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            logging.info("Data fetched successfully from the database.")
            return df
    except SQLAlchemyError as e:
        logging.error(f"Error fetching data from the database: {e}")
        raise


def save_dataframe_to_html(df, filepath):
    """
    Save the DataFrame to an HTML file.

    Args:
        df (DataFrame): The DataFrame to save.
        filepath (str): The file path to save the HTML file.
    """
    try:
        df.to_html(filepath)
        logging.info(f"DataFrame saved to HTML file at {filepath}.")
    except Exception as e:
        logging.error(f"Error saving DataFrame to HTML: {e}")
        raise


def main():
    """
    Main function to execute the script.
    """
    engine = create_engine_connection(DATABASE_URI)
    df = fetch_data_to_dataframe(engine, QUERY)
    save_dataframe_to_html(df, 'templates/sql-data.html')


if __name__ == '__main__':
    main()
