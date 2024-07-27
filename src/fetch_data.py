from typing import List, Literal, Dict, Set
from pathlib import Path
from os.path import join
from datetime import datetime, timedelta
import json
import logging

import pandas
# import matplotlib.pyplot as plt
# from mplfinance.original_flavor import candlestick_ohlc
# import matplotlib.dates as mdates
import pandas as pd

import yfinance as yf

from src.config import Config

LastUpdateDict = Dict[str, datetime]


def get_existing_symbols_in_db(h5_file_path) -> set:
    """
    Open an HDF5 file and return a list of symbols from the 'Code' column.

    Parameters:
        h5_file_path (str): The path to the HDF5 file.

    Returns:
        list: A list of symbols if successful; otherwise, an empty list.
    """

    h5_file_path = Path(h5_file_path)

    # Check if the file exists
    if not h5_file_path.exists():
        logging.info(f"The file {h5_file_path} does not exist.")
        return set()

    try:
        with pd.HDFStore(str(h5_file_path), 'r') as store:
            existing_symbols = {key[1:] for key in store.keys()}

        return existing_symbols

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return set()

def fetch_data_from_yahoo_finance(tickers: List[str]|Set[str],
                                  start_date: datetime,
                                  end_date: datetime,
                                  interval: Literal['5m', '1d']) -> Dict[str, pd.DataFrame]:
    if tickers is None or len(tickers) == 0:
        return {}
    df = yf.download(tickers, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'),
                     interval=interval, auto_adjust=True, keepna=True)
    if isinstance(df.columns, pandas.MultiIndex):
        tickers_found = df.columns.get_level_values(level=1).unique().tolist()
        dfs = {}
        for ticker in tickers_found:
            dfs[ticker] = df.xs(key=ticker, level=1, axis='columns')
        return dfs
    else:
        return df


def get_earlier_date(dic: Dict[str, str]) -> datetime:
    if dic is None or len(dic) == 0:
        return datetime.min
    dates = [datetime.fromisoformat(str_date) for str_date in dic.values()]
    return min(dates)


def fetch_price_data(existing_symbols: set, all_stocks_symbols: set, data_dir, log_file, interval: Literal['5m', '1d'] = '1D', hours_to_skip=24):
    match interval:
        case '5m': start_time = datetime.now() - timedelta(days=59)
        case '1d' | '1D': start_time = datetime.now() - timedelta(days=5 * 365)
        case _: raise ValueError(f"Invalid interval: {interval}")
    # Load existing logs if available
    try:
        with open(log_file, 'r') as f:
            symbol_time_dict = json.load(f)
    except FileNotFoundError:
        symbol_time_dict = {}
    existing_symbols_start_time = max(start_time, get_earlier_date(symbol_time_dict))

    # Create the directory if it doesn't exist
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    hdf5_file_path = join(data_dir, "eod_price_data.h5")

    new_symbols = all_stocks_symbols.difference(existing_symbols)
    existing_symbols_dfs = fetch_data_from_yahoo_finance(existing_symbols, existing_symbols_start_time, datetime.now(), interval=interval)
    new_symbols_dfs = fetch_data_from_yahoo_finance(new_symbols, start_time, datetime.now(), interval=interval)

    # Update log
    for symbol in all_stocks_symbols:
        symbol_time_dict[symbol] = datetime.now().isoformat()
    with open(log_file, 'w') as f:
        json.dump(symbol_time_dict, f)

    all_symbols_dfs = existing_symbols_dfs.copy()
    all_symbols_dfs.update(new_symbols_dfs)
    # Update the HDF5 file every 100 symbols
    with pd.HDFStore(hdf5_file_path, mode='a') as store:
        for symbol, data in all_symbols_dfs.items():
            store.put(symbol, data)

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

    data_dir = Config.data_dir
    log_file_path = "../data/symbol_price_data_time_log.json"
    tickers_filepath = Config.tickers_filepath
    h5_filename = Config.eod_file_path
    interval: Literal['5m', '1d'] = '1d'

    existing_symbols = get_existing_symbols_in_db(data_dir + h5_filename)
    all_symbols = set(Config.get_tickers_list())
    logging.info(f'Processing {len(all_symbols)} symbols')

    fetch_price_data(existing_symbols, all_symbols,  data_dir, log_file_path, interval=interval, hours_to_skip=72)
    #
    # symbol = 'TSLA'
    # hdf5_file_path = f"{data_dir}/eod_price_data.h5"
    #
    # symbol = 'TSLA'
    # hdf5_file_path = f"{data_dir}/eod_price_data.h5"
    # start_date = '2023-01-01'
    # end_date = '2023-10-23'
    # plot_candlestick_with_volume(symbol, hdf5_file_path, start_date, end_date)
