from pathlib import Path
from typing import Tuple, Any

import numpy as np
import pandas as pd
import vectorbt as vbt

from src.config import Config


def ma_150_crossed(df: pd.DataFrame)-> Tuple[Any, Any]:
    ma = vbt.MA.run(df['close'], 150)
    enter_signal = ma.ma_crossed_above(df['close'])

    atr = vbt.ATR.run(low=df['low'], close=df['close'], high=df['high'], window=14)
    # Track the highest price since entry for trailing stop calculation
    # Initialize trailing stop price to NaN (no stop initially)
    trailing_stop = pd.Series(np.nan, index=df.index)
    # Calculate trailing stop using ATR (set 2x ATR as the trailing stop distance)
    stop_multiple = 3  # Trailing stop as a multiple of ATR
    for i in range(1, len(df)):
        if enter_signal.iloc[i]:
            trailing_stop.iloc[i] = df['low'].iloc[i] - stop_multiple * atr.atr.iloc[i]
        else:
            # Carry forward the last stop
            trailing_stop.iloc[i] = max(trailing_stop.iloc[i - 1],
                                        df['low'].iloc[i] - stop_multiple * atr.atr.iloc[i])

    # Create exit signal: Price touches or goes below the trailing stop
    exit_signal = df['close'] <= trailing_stop  # Price hits trailing stop

    return enter_signal, exit_signal

def cci_cross_zero2(df: pd.DataFrame) -> Tuple[Any, Any]:
    cci = vbt.pandas_ta('cci').run(low=df['low'], close=df['close'], high=df['high'], window=14)
    cci_values = cci.cci

    # Initialize entry and exit signals
    entry_signal = np.zeros_like(cci_values, dtype=bool)
    exit_signal = np.zeros_like(cci_values, dtype=bool)

    # Create masks for CCI conditions
    below_neg100 = cci_values < -100
    above_pos100 = cci_values > 100
    above_zero = cci_values >= 0
    below_zero = cci_values <= 0

    # Track the state of the CCI
    in_below_neg100 = False
    in_above_pos100 = False

    in_position = False
    
    i = 1
    while i < len(cci_values)-1:
        # Entry condition
        if below_neg100[i - 1]:
            while (i < len(cci_values)) and (cci_values[i] > cci_values[i - 1]):
                if above_zero[i]:
                    entry_signal[i] = True
                    exit_signal[i+1] = True
                    break
                else:
                    i+=1
        i+=1

        # Exit condition
        # if not in_above_pos100 and above_pos100[i - 1]:
        #     in_above_pos100 = True
        # if in_above_pos100 and below_zero[i]:
        #     exit_signal[i] = True
        #     in_above_pos100 = False

    # Convert signals back to pandas Series
    entry_signal_series = pd.Series(entry_signal, index=cci_values.index)
    exit_signal_series = pd.Series(exit_signal, index=cci_values.index)

    return entry_signal_series, exit_signal_series

def cci_cross_zero(df: pd.DataFrame) -> Tuple[Any, Any]:
    window_size = 5  # Number of days to track upward movement

    # Entry: Check if CCI was below -100 at the start of the window and is now >= 0
    def custom_cci_entry(cci_series, window):
        below_neg_100 = cci_series.shift(window) < -100  # Was below -100 'window' days ago
        rising_trend = (cci_series.diff().rolling(window).sum()) > 0  # Gradually increasing
        crossing_zero = cci_series >= 0  # Has reached or exceeded 0
        return below_neg_100 & rising_trend & crossing_zero

    # Exit: Check if CCI was above 100 at the start of the window and is now <= 0
    def custom_cci_exit(cci_series, window):
        above_100 = cci_series.shift(window) > 100  # Was above 100 'window' days ago
        falling_trend = (cci_series.diff().rolling(window).sum()) < 0  # Gradually decreasing
        crossing_zero = cci_series <= 0  # Has reached or dropped below 0
        return above_100 & falling_trend & crossing_zero

    cci = vbt.pandas_ta('cci').run(low=df['low'], close=df['close'], high=df['high'], window=14)
    entry_signal = custom_cci_entry(cci.cci, window_size)
    exit_signal = custom_cci_exit(cci.cci, window_size)
    return entry_signal, exit_signal

def moving_avg_breakout(df: pd.DataFrame, atr_mult: float = 1.5) -> Tuple[Any, Any]:
    ma_period = 20
    lookback = 10  # Days to confirm a downtrend

    atr = vbt.ATR.run(low=df['low'], close=df['close'], high=df['high'], window=14)
    atr = atr.atr

    ma = vbt.MA.run(df['close'], window=ma_period)
    df['MA'] = ma.ma # df['close'].rolling(ma_period).mean()

    # Relative drop from lookback days ago
    # past_price = df['close'].shift(lookback)
    # drop_amt = past_price - df['close']
    # atr_thresh = atr * atr_mult
    # steep_enough = drop_amt > atr_thresh

    # Condition 1: Downtrend - Close below MA for last `lookback` bars
    df['Below_MA'] = df['close'] < df['MA']
    df['Was_Downtrend'] = df['Below_MA'].rolling(lookback).sum() == lookback
    df['Was_Downtrend'] = df['Was_Downtrend'] #& steep_enough

    # Condition 2: Breakout - Close crosses above MA
    df['Breakout'] = (df['close'] > df['MA']) & (df['close'].shift(1) <= df['MA'].shift(1))

    # Condition 3: Confirm breakout only if it follows a downtrend
    df['Confirmed_Breakout'] = df['Breakout'] & df['Was_Downtrend'].shift(1)

    # Forward-fill entry to mark a "holding" state
    holding = df['Confirmed_Breakout'].cumsum()
    holding[df['close'] < df['MA']] = np.nan  # Exit when below MA
    holding = holding.ffill().notna() & df['Confirmed_Breakout'].cumsum().gt(0)  # Filter active trades


    df['Cross_down'] = holding & df['Below_MA'] & (df['close'].shift(1) >= df['MA'].shift(1))

    entry_signal = df['Confirmed_Breakout']
    exit_signal = df['Cross_down']
    exit_signal = exit_signal.astype(bool).fillna(False)
    
    return entry_signal, exit_signal



def main():
    h5_file_path = Path(Config.eod_price_data_stooq_path)
    ticker = 'NVDA'
    with pd.HDFStore(h5_file_path.resolve(), 'r+') as store:
        df = store.get(ticker)
        df.columns = map(str.lower, df.columns)
        
        df = df[df.index >= pd.Timestamp.now() - pd.DateOffset(years=15)]

        # pf = vbt.Portfolio.from_signals(price, entries, exits, init_cash=100)
        # print(pf.total_profit())
        entry_signal, exit_signal = moving_avg_breakout(df)
        # entry_signal, exit_signal = cci_cross_zero2(df)
        # entry_signal, exit_signal = ma_150_crossed(df)
        
        portfolio = vbt.Portfolio.from_signals(df['close'], entries=entry_signal, exits=exit_signal, freq='1d')
        print(portfolio.stats())
        print(portfolio.total_profit())
        portfolio.plot().show()

        # windows = np.arange(2, 101)
        # fast_ma, slow_ma = vbt.MA.run_combs(price, window=windows, r=2, short_names=['fast', 'slow'])
        # entries = fast_ma.ma_crossed_above(slow_ma)
        # exits = fast_ma.ma_crossed_below(slow_ma)
        #
        # pf_kwargs = dict(size=np.inf, fees=0.001, freq='1D')
        # pf = vbt.Portfolio.from_signals(price, entries, exits, **pf_kwargs)
        #
        # fig = pf.total_return().vbt.heatmap(
        #     x_level='fast_window', y_level='slow_window', symmetric=True,
        #     trace_kwargs=dict(colorbar=dict(title='Total return', tickformat='%')))
        # fig.show()


if __name__ == '__main__':
    main()