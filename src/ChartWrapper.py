from pathlib import Path
from typing import Dict

import pandas as pd
import pandas_ta as ta
from lightweight_charts import Chart
from lightweight_charts.abstract import Line
from pandas import HDFStore

from config import Config
from backtest.backtest_vectorbt import ma_150_crossed, cci_cross_zero2, moving_avg_breakout

class ChartWrapper:
    def __init__(self, store: HDFStore):
        self.chart = Chart(inner_width=1, inner_height=0.7)
        self.store = store
        self.existing_symbols = {key[1:] for key in store.keys()}
        self.current_indicators: Dict[str, Line] = {}

        self.chart.legend(True)
        self.chart.events.search += self.on_search
        self.chart.topbar.textbox('symbol', 'AAPL')

        self.subchart = self.chart.create_subchart(width=1, height=0.3, sync=True)

        self.set_data('AAPL')


    def set_data(self, symbol: str) -> bool:
        if symbol not in self.existing_symbols:
            return False

        df: pd.DataFrame = store.get(symbol)

        self.chart.watermark(symbol)

        df.columns = df.columns.str.lower()
        self.chart.set(df)

        self._draw_indicators(df)
        self._draw_signals(df)

        return True

    def _draw_indicators(self, df: pd.DataFrame):
        self._draw_smas(df)
        self._draw_cci(df)

    def _draw_signals(self, df: pd.DataFrame):
        # Generate signals using both strategies
        # ma_enter, ma_exit = ma_150_crossed(df)
        cci_enter, cci_exit = moving_avg_breakout(df)

        # Create markers for enter signals
        enter_markers = [
            {
                'time': time, 'position': 'below', 'color': 'green', 'shape': 'circle', 'text': ''
            }
            for time in df.index[cci_enter]
        ]

        # Create markers for exit signals
        exit_markers = [
            {
                'time': time, 'position': 'below', 'color': 'red', 'shape': 'circle', 'text': ''
            }
            for time in df.index[cci_exit]
        ]

        # Add markers to the chart
        self.chart.marker_list(exit_markers)
        self.chart.marker_list(enter_markers)

    def _draw_smas(self, df: pd.DataFrame):
        sma = df.ta.sma(length=20).to_frame()
        sma = sma.rename(columns={"Date": "time", "SMA_20": "value"})
        sma = sma.dropna()

        # add sma line
        sma_line = self.current_indicators.setdefault('sma_20', self.chart.create_line())
        sma_line.set(sma)

    def _draw_cci(self, df: pd.DataFrame):
        cci = df.ta.cci(length=14).to_frame()
        cci = cci.rename(columns={"Date": "time", cci.columns[-1]: "value"})

        # get/create cci lines
        cci_line = self.current_indicators.setdefault('cci_line', self.subchart.create_line())

        cci_upper_line = self.current_indicators.setdefault('cci_upper_line', self.subchart.create_line(color='red'))
        cci_upper_df = cci.copy()
        cci_upper_df[cci_upper_df.columns[-1]] = 100
        cci_upper_line.set(cci_upper_df)

        cci_middle_line = self.current_indicators.setdefault('cci_middle_line', self.subchart.create_line(color='white', width=1))
        cci_middle_df = cci.copy()
        cci_middle_df[cci_middle_df.columns[-1]] = 0
        cci_middle_line.set(cci_middle_df)

        cci_lower_line = self.current_indicators.setdefault('cci_lower_line', self.subchart.create_line(color='green'))
        cci_lower_df = cci.copy()
        cci_lower_df[cci_lower_df.columns[-1]] = -100
        cci_lower_line.set(cci_lower_df)

        cci_line.set(cci)

    def show(self):
        self.chart.show(block=True)

    def on_search(self, chart: Chart, searched_string: str):
        found = self.set_data(searched_string)
        if not found:
            return
        chart.topbar['symbol'].set(searched_string)

    def close(self):
        self.store.close()

if __name__ == '__main__':

    # Columns: time | open | high | low | close | volume
    # df = pd.read_csv('ohlcv.csv')
    tickers = Config.get_tickers_list()
    h5_file_path = Path(Config.eod_price_data_stooq_path)
    with pd.HDFStore(h5_file_path.resolve(), 'r+') as store:
        chart_wrapper = ChartWrapper(store)
        chart_wrapper.show()

        # chart.legend(True)
        # chart.topbar.textbox('symbol', 'AAPL')
        # chart.show(block=True)
