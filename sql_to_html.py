from flight_fetching import *
from sqlalchemy import create_engine


# Database connection string
DATABASE_URI = 'mysql+pymysql://root:VavaChab!2!6@localhost:3306/flights_data'

# Create database connection
engine = create_engine(DATABASE_URI)

# SQL query to fetch data from a specific table
QUERY = """
SELECT * FROM flights_subset;
"""

# Fetch the data into a DataFrame
with engine.connect() as connection:
    df = pd.read_sql(QUERY, connection)

# Display the DataFrame
df.to_html('templates/sql-data.html')