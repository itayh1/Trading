from typing import List

import yaml


class Config:
    data_dir = '../data/'
    eod_file_path = f"{data_dir}/eod_price_data.h5"
    five_m_file_path = f"{data_dir}/5m_price_data.h5"
    tickers_filepath = "tickers_list.yaml"

    @staticmethod
    def get_tickers_list() -> List[str]:
        with open(Config.tickers_filepath, 'r') as f:
            data = yaml.safe_load(f)
            return data['tickers']