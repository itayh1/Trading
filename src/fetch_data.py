from typing import List, Literal, Dict, Set
from pathlib import Path
from os.path import join
from datetime import datetime, timedelta
import json
import logging

# import matplotlib.pyplot as plt
# from mplfinance.original_flavor import candlestick_ohlc
# import matplotlib.dates as mdates
import pandas as pd

import yfinance as yf

from src.config import Config


def get_symbols(h5_file_path, key='US'):
    """
    Open an HDF5 file and return a list of symbols from the 'Code' column.

    Parameters:
        h5_file_path (str): The path to the HDF5 file.
        key (str): The key to use when reading the HDF5 file. Default is 'exchanges'.

    Returns:
        list: A list of symbols if successful; otherwise, an empty list.
    """

    h5_file_path = Path(h5_file_path)

    # Check if the file exists
    if not h5_file_path.exists():
        logging.info(f"The file {h5_file_path} does not exist.")
        return []

    try:
        # Read the DataFrame from the HDF5 file
        df = pd.read_hdf(h5_file_path, key=key)

        # Check if 'Code' column exists
        if 'Code' not in df.columns:
            logging.info(f"The 'Code' column does not exist in the DataFrame.")
            return []

        # Get the list of symbols from the 'Code' column
        symbols = df['Code'].tolist()

        return symbols

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return []

def fetch_data_from_yahoo_finance(tickers: List[str]|Set[str],
                                  start_date: datetime,
                                  end_date: datetime,
                                  interval: Literal['5m', '1d']) -> Dict[str, pd.DataFrame]:
    if tickers is None or len(tickers) == 0:
        return {}
    df = yf.download(tickers, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'),
                     interval=interval, auto_adjust=True, keepna=True)
    tickers_found = df.columns.get_level_values(level=1).unique().tolist()
    dfs = {}
    for ticker in tickers_found:
        dfs[ticker] = df.xs(key=ticker, level=1, axis='columns')
    return dfs


def fetch_price_data(stock_symbols, data_dir, log_file, hours_to_skip=24):
    # Load existing logs if available
    try:
        with open(log_file, 'r') as f:
            symbol_time_dict = json.load(f)
    except FileNotFoundError:
        symbol_time_dict = {}

    all_data = {}
    count = 0

    # Create the directory if it doesn't exist
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    hdf5_file_path = join(data_dir, "eod_price_data.h5")

    existing_symbols = set()

    try:
        with pd.HDFStore(hdf5_file_path, 'r') as store:
            existing_symbols = {key[1:] for key in store.keys()}
    except FileNotFoundError:
        pass

    existing_symbols.update(stock_symbols)
    dfs = fetch_data_from_yahoo_finance(existing_symbols, datetime.now() - timedelta(days=59), datetime.now(), interval='5m')
    for ticker, df in dfs.items():
        last_downloaded_time = symbol_time_dict.get(ticker, None)

        # if last_downloaded_time:
        #     last_downloaded_time = datetime.fromisoformat(last_downloaded_time)
        #     time_since_last_download = datetime.now() - last_downloaded_time
        #     if time_since_last_download < timedelta(hours=hours_to_skip):
        #         logging.info(f"Data for symbol {ticker} was downloaded recently. Skipping...")
        #         continue

        # logging.info(f"{symbol}: Downloading from EODHD...")
        # url = f'https://eodhd.com/api/eod/{symbol}.US?api_token={api_token}&fmt=csv'
        # response = requests.get(url)
        # if response.status_code != 200:
        #     logging.error(f"Failed to fetch data for symbol {symbol}. HTTP Status Code: {response.status_code}")
        #     continue
        #
        # csv_data = StringIO(response.text)
        # df = pd.read_csv(csv_data)

        # if 'Date' not in df.columns:
        #     logging.error(f"No 'Date' column in the data for symbol {symbol}. Skipping...")
        #     continue

        # Set 'Date' as DateTime index
        # df['Date'] = pd.to_datetime(df['Date'])
        # df.set_index('Date', inplace=True)

        # Add the DataFrame to the all_data dictionary
        all_data[ticker] = df

        # Update log
        symbol_time_dict[ticker] = datetime.now().isoformat()
        with open(log_file, 'w') as f:
            json.dump(symbol_time_dict, f)

        count += 1

        # Update the HDF5 file every 100 symbols
        if count % 100 == 0:
            with pd.HDFStore(hdf5_file_path, mode='a') as store:
                for ticker, data in all_data.items():
                    store.put(ticker, data)
            logging.info(f"Saved {list(all_data.keys())} to HDF5 file.")
            all_data = {}

    # Save remaining data to HDF5
    if all_data:
        with pd.HDFStore(hdf5_file_path, mode='a') as store:
            for ticker, data in all_data.items():
                store.put(ticker, data)
        logging.info(f"Saved remaining {list(all_data.keys())} to HDF5 file.")

    logging.info("Finished updating HDF5 file.")


# def plot_candlestick_with_volume(symbol, hdf5_file_path, start_date, end_date):
#     with pd.HDFStore(hdf5_file_path, 'r') as store:
#         if f'/{symbol}' in store.keys():
#             df = store.get(symbol)
#             logging.info("Data fetched successfully.")
#
#             # Filter the DataFrame based on the start and end dates
#             df = df[start_date:end_date]
#
#             # Ensure the DataFrame is sorted by date
#             df = df.sort_index()
#
#             # Convert date to a format that can be used by candlestick_ohlc
#             df['Date'] = df.index.map(mdates.date2num)
#
#             # Create subplots: one for candlestick, one for volume
#             fig, ax1 = plt.subplots(figsize=(10, 6))
#             ax2 = ax1.twinx()
#
#             # Plot candlestick chart
#             ohlc = df[['Date', 'Open', 'High', 'Low', 'Adjusted_close']].copy()
#             candlestick_ohlc(ax1, ohlc.values, width=0.6, colorup='green', colordown='red')
#             ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
#             ax1.set_xlabel('Date')
#             ax1.set_ylabel('Price')
#             ax1.set_title(f'Candlestick chart for {symbol} ({start_date} to {end_date})')
#             ax1.grid(True)
#
#             # Plot volume
#             ax2.bar(df.index, df['Volume'], color='gray', alpha=0.3)
#             ax2.set_ylabel('Volume')
#
#             plt.show()
#
#         else:
#             logging.info(f"No data found for symbol: {symbol}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    data_dir = '../data/'
    log_file_path = "../data/symbol_price_data_time_log.json"
    tickers_filepath = 'tickers_list.yaml'

    symbols = get_symbols(data_dir + 'symbols.h5', key='US')
    symbols = set(symbols + Config.get_tickers_list())
    logging.info(f'Processing {len(symbols)} symbols')

    fetch_price_data(symbols, data_dir, log_file_path, hours_to_skip=72)
    #
    # symbol = 'TSLA'
    # hdf5_file_path = f"{data_dir}/eod_price_data.h5"
    #
    # symbol = 'TSLA'
    # hdf5_file_path = f"{data_dir}/eod_price_data.h5"
    # start_date = '2023-01-01'
    # end_date = '2023-10-23'
    # plot_candlestick_with_volume(symbol, hdf5_file_path, start_date, end_date)
