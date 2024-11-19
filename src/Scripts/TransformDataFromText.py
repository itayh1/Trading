import os

import pandas as pd
from pandas import HDFStore

from src.config import Config


# based on stock data downloaded from "https://stooq.com/db/h/".
# transform the selected stocks from the raw text data into dataframes

def parse_stock_data(directory_path, tickers):
    """
    Parses stock data from files in the given directory, loading only selected tickers into dataframes.

    Parameters:
        directory_path (str): Path to the directory containing stock data files.
        tickers (list): List of stock tickers to parse.

    Returns:
        dict: A dictionary where keys are tickers and values are Pandas dataframes of stock data.
    """
    tickers =  [ticker.lower() for ticker in tickers]
    stock_data = {}

    for root, _, files in os.walk(directory_path):
        for file in files:
            # Extract ticker from the file name (assuming file name contains ticker)
            ticker = file.split('.')[0]

            # Process file only if the ticker is in the provided tickers list
            if ticker in tickers:
                file_path = os.path.join(root, file)

                try:
                    # Read the data into a Pandas DataFrame
                    df = pd.read_csv(
                        file_path,
                        names=['TICKER', 'PER', 'DATE', 'TIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL', 'OPENINT'],
                        dtype={'DATE': str, 'TIME': str},
                        header=None, skiprows=1
                    )

                    df['DATE'] = pd.to_datetime(df['DATE'].astype(str) + df['TIME'].astype(str), format='%Y%m%d%H%M%S')
                    df = df[['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL']]
                    df.set_index('DATE', inplace=True)

                    # Store the dataframe in the dictionary
                    stock_data[ticker.upper()] = df
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")

    return stock_data


def print_hdfs_tickers(hdfs_file_path: str):
    with pd.HDFStore(Config.eod_price_data_stooq_path, mode='r') as store:
        print(f"Symbols and row counts in {hdfs_file_path}:")
        for key in store.keys():
            # Retrieve the number of rows for each symbol
            nrows = store.get_storer(key).nrows
            print(f"{key}: {nrows} rows")

def main():
    # path = 'data/daily/nasdaq stocks/1/aapl.us.txt'
    # df = pd.read_csv(
    #     path,
    #     names=['TICKER', 'PER', 'DATE', 'TIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL', 'OPENINT'],
    #     header=None,
    #     skiprows=1,
    # )
    # df = df[['DATE', 'TIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL']]
    # df['DATE'] = pd.to_datetime(df['DATE'], format='%Y%m%d')
    # df.set_index('DATE', inplace=True)
    # # df.reset_index(inplace=True)
    # print(df.head())
    h5_file_path = 'eod_price_data_stooq.h5'
    directory_path = "data/daily"  # Replace with the path to your directory
    selected_tickers = ['AAPL', 'GOOGL', 'MSFT']  # Replace with your desired tickers

    stock_dataframes = parse_stock_data(directory_path, selected_tickers)

    with pd.HDFStore(Config.eod_price_data_stooq_path, mode='w') as store:
        for symbol, data in stock_dataframes.items():
            store.put(symbol, data, format='table', append=True, data_columns=True)

    # print_hdfs_tickers(Config.eod_price_data_stooq_path)


if __name__ == '__main__':
    main()