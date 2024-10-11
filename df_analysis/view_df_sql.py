import datetime
import pandas as pd
from sqlalchemy import create_engine

todays_date = datetime.date.today().strftime('%Y-%m-%d')

QUERY = f"""
SELECT flight, 
       DATE_FORMAT(FROM_UNIXTIME(planned), '%%H:%%i') AS planned_time, 
       destination,
       gate, 
       DATE_FORMAT(FROM_UNIXTIME(revised), '%%H:%%i') AS revised_time
FROM flights
WHERE CAST(gate AS UNSIGNED) BETWEEN 62 AND 68
  AND DATE(FROM_UNIXTIME(planned)) = CURDATE()
ORDER BY planned_time;"""

connection_string = 'mysql+pymysql://root:VavaChab!2!6@localhost:3306/flights_data'


class ViewDF:
    def __init__(self):
        self.engine = create_engine(connection_string)
        self.df = pd.read_sql(QUERY, self.engine)

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
    data = ViewDF()
    # data.store_to_db('view_table', connection_string)
    df = data.df
    df.to_html('template/view_df-html.html')
    print(df.columns)


if __name__ == '__main__':
    main()
