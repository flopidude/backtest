import os
import unittest
import asyncio

import polars as pl

from binance_data import BinanceDataDownloader
from datetime import datetime

class TestBinanceDataDownloader(unittest.TestCase):
    def test_download_one_ticker(self):
        downloader = BinanceDataDownloader()
        data = asyncio.run(downloader.download_one_ticker("BTCUSDT", datetime(2020,1,1), datetime(2020,1,31),"1d"))
        print(data)
        print(data.columns)
        self.assertTrue(type(data) == pl.DataFrame and data.shape[0] > 0)

    def test_update_file(self):
        downloader = BinanceDataDownloader()
        asyncio.run(downloader.ensure_tickers(["BTCUSDT"], ["1m"]))
        self.assertTrue(os.path.exists("./tickers/BTCUSDT-1m.csv"))

    def test_invalid_params(self):
        downloader = BinanceDataDownloader()
        with self.assertRaises(Exception):
            data = asyncio.run(downloader.download_one_ticker("INVALID", datetime(2020,1,1), datetime(2020,1,31),"1w"))


if __name__ == '__main__':
    unittest.main()