from typing import List, Any

import pandas as pd

from flight_fetching import *

url = 'https://www.admtl.com/en/admtldata/api/flight?type=departure&sort=field_planned&direction=ASC&rule=24h'

columns_of_interest = ['id', 'type', 'flight', 'planned', 'revised', 'company', 'compagny_without_accent',
                       'destination', 'gate']

new_columns_of_interest = []

def clean_list(list):
    new_list = []
    for item in list:
        new_list.append(item.strip())
    return new_list


def filter_flights_by_gate_range(raw_df, min_gate=62, max_gate=68):
    # Convert 'gate' column to numeric, coercing errors to NaN, then downcast to integer
    raw_df['gate'] = pd.to_numeric(raw_df['gate'], downcast='integer', errors='coerce')

    # Drop rows where 'gate' is NaN
    raw_df = raw_df.dropna(subset=['gate'])

    # Ensure the 'gate' column is converted to integers
    raw_df['gate'] = raw_df['gate'].astype(int)

    # Filter the DataFrame for gate numbers within the specified range
    relevant_flights = raw_df.loc[(raw_df['gate'] >= min_gate) & (raw_df['gate'] <= max_gate)]

    return relevant_flights


def main():
    raw_df = process_flights_to_df(url)

    filtered_df = filter_flights_by_gate_range(raw_df)

    extra_cols = filtered_df['destination'].tolist()

    extra_cols = clean_list(extra_cols)

    #print(extra_cols)
    new_df_header = columns_of_interest + extra_cols
    new_df_header.remove('compagny_without_accent')
    new_df_header.insert(0, 'total_num_flights')
    new_df_header.insert(1, 'date')
    #print(new_df_header)

    df = pd.DataFrame(data=None, columns=new_df_header)
    print(df)


if __name__ == '__main__':
    main()
