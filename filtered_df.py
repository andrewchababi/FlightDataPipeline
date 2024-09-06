from df_to_html import *

class working_df:
    url = 'https://www.admtl.com/en/admtldata/api/flight?type=departure&sort=field_planned&direction=ASC&rule=24h'
    df = pd.DataFrame()


    def __init__(self):
        self.df = process_flights_to_df(self.url)
        print(type(self.df))
        self.df = filter_flights_by_gate_range(self.df)
        print(type(self.df))
        self.df = filter_df_columns(self.df)
        print(type(self.df))
        self.df = fix_time_columnn(self.df)
        print(type(self.df))

    def display_df(self):
        print(self.df)
    def display_columns_df(self):
        print(self.df.columns)
    def display_df_header(self):
        print(self.df.head())


class analysis_df:
    def __init__(self):
        pass

def main():
    df = working_df()
    df.display_df()
    df.display_columns_df()
    df.display_df_header()

if __name__ == '__main__':
    main()
