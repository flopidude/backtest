import os
import time
import unittest
import asyncio

import polars as pl

from binance_data import BinanceDataDownloader, BinanceDataProvider
from datetime import datetime

class TestBinanceDataDownloader(unittest.TestCase):
    # def setUp(self):
    #     self.startTime = time.time()
    #
    # def tearDown(self):
    #     t = time.time() - self.startTime
    #     print('%s: %.3f' % (self.id(), t))
    def test_downloader_one_ticker(self):
        downloader = BinanceDataDownloader()
        data = asyncio.run(downloader.download_one_ticker("BTCUSDT", datetime(2020,1,1), datetime(2024,3,10),"1d"))
        # print(data)
        self.assertTrue(type(data) == pl.DataFrame and data.shape[0] > 0)

    def test_downloader_one_ticker_invalid(self):
        downloader = BinanceDataDownloader()
        with self.assertRaises(Exception):
            data = asyncio.run(downloader.download_one_ticker("INVALID", datetime(2020,1,1), datetime(2020,1,31),"1w"))

    def test_binance_data_provider(self):
        provider = BinanceDataProvider(["ETHUSDT", "BTC/USDT:USDT", "RUNE/USDT:USDT"], ["1m"])
        asyncio.run(provider.update_tickers(["ETHUSDT", "BTC/USDT:USDT"], ["1m"]))
        # print(provider.cached_dataframes["1m"]["ETHUSDT"])
        self.assertTrue(os.path.exists("./tickers/ETHUSDT-1m.csv"))

    def test_binance_data_provider_async(self):
        provider = BinanceDataProvider(["ETHUSDT", "BTC/USDT:USDT", "RUNE/USDT:USDT"], ["1m"])
        asyncio.run(provider.update_tickers_async(["ETHUSDT", "BTC/USDT:USDT"], ["1m"]))
        # print(provider.cached_dataframes["1m"]["ETHUSDT"])
        self.assertTrue(os.path.exists("./tickers/ETHUSDT-1m.csv"))

    # def test_binance_data_provider_ccxt(self):
    #     provider = BinanceDataProvider(["RUNE/USDT:USDT"], ["1m"])
    #     asyncio.run(provider.update_tickers(["RUNE/USDT:USDT"], ["1m"]))
    #     self.assertTrue(os.path.exists("./tickers/RUNEUSDT-1m.csv"))




if __name__ == '__main__':
    unittest.main()