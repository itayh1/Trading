from pathlib import Path

import pandas as pd
import pandas_ta as ta
from lightweight_charts import Chart
from lightweight_charts.abstract import Line
from pandas import HDFStore

from config import Config

class ChartWrapper:
    def __init__(self, store: HDFStore):
        self.chart = Chart()
        self.store = store
        self.existing_symbols = {key[1:] for key in store.keys()}
        self.current_smas: list[Line] = []

        self.chart.legend(True)
        self.chart.events.search += self.on_search
        self.chart.topbar.textbox('symbol', 'AAPL')

        # self.set_data('AAPL')

    def set_data(self, symbol: str) -> bool:
        if symbol not in self.existing_symbols:
            return False

        df = store.get(symbol)

        # clear old chart
        for sma in self.current_smas:
            sma.delete()
        self.current_smas.clear()
        self.chart.watermark(symbol)

        # prepare indicator values
        sma = df.ta.sma(length=20).to_frame()
        sma = sma.reset_index()
        sma = sma.rename(columns={"Date": "time", "SMA_20": "value"})
        sma = sma.dropna()

        df = df.reset_index()
        df.columns = df.columns.str.lower()
        self.chart.set(df)

        # add sma line
        line = self.chart.create_line()
        line.set(sma)
        self.current_smas.append(line)

        return True

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
    h5_file_path = Path(Config.eod_file_path)
    with pd.HDFStore(h5_file_path.resolve(), 'r+') as store:
        chart_wrapper = ChartWrapper(store)
        chart_wrapper.show()

        # chart.legend(True)
        # chart.topbar.textbox('symbol', 'AAPL')
        # chart.show(block=True)
