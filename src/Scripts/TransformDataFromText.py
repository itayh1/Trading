import os
import re
import subprocess
from zipfile import ZipFile
from os import path

import pandas as pd
from pandas import HDFStore
from tqdm import tqdm

from src.config import Config


# based on stock data downloaded from "https://stooq.com/db/h/".
# transform the selected stocks from the raw text data into dataframes
def parse_stock_data(directory_path, tickers=None):
    """
    Parses stock data from files in the given directory, loading only selected tickers into dataframes.

    Parameters:
        directory_path (str): Path to the directory containing stock data files.
        tickers (list): List of stock tickers to parse.

    Returns:
        dict: A dictionary where keys are tickers and values are Pandas dataframes of stock data.
    """
    ticker_regex = re.compile(r"^[a-zA-Z]{1,5}(\.[a-zA-Z]{1,2})?$")
    if tickers is None:
        tickers = []
    else:
        tickers =  [ticker.lower() for ticker in tickers]
    stock_data = {}


    total_files = sum(len(files) for _, _, files in os.walk(directory_path))
    with tqdm(total=total_files, desc="Parsing stock data") as pbar:
        for root, _, files in os.walk(directory_path):
            for file in files:
                pbar.update(1)
                if not file.endswith('.txt'): continue

                # Extract ticker from the file name (assuming file name contains ticker)
                ticker = file.split('.')[0]

                if not ticker_regex.match(ticker): continue

                # Process file only if the ticker is in the provided tickers list
                # if ticker in tickers:
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

def extract_zip_file(zip_filepath: str, extract_path: str) -> (bool, str):
    zip_file_path = path.join(Config.data_dir, zip_filepath)

    if not os.path.exists(zip_file_path):
        return (False, "")

    dest_folder = path.join(Config.data_dir, extract_path)
    if os.path.exists(dest_folder):
        return (True, dest_folder)

    try:
        subprocess.run(["unzip", zip_file_path, "-d", dest_folder])
    except Exception as e:
        return (False, e)

    return (True, dest_folder)

def main():
    directory_path = path.join(Config.data_dir,'d_us')
    # (succeed, directory_path) = extract_zip_file('d_us_txt.zip', 'd_us')
    # if not succeed:
    #     directory_path = "data/"  # Replace with the path to your directory
    # selected_tickers = ['AAPL', 'GOOGL', 'MSFT']  # Replace with your desired tickers

    stock_dataframes = parse_stock_data(directory_path, None)

    with pd.HDFStore(Config.eod_price_data_stooq_path, mode='w') as store:
        for symbol, data in tqdm(stock_dataframes.items(), desc="saving parsed data into HDF5 store"):
            store.put(symbol, data, format='table', append=True, data_columns=True)

    # print_hdfs_tickers(Config.eod_price_data_stooq_path)


if __name__ == '__main__':
    main()