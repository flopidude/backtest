import os
import unittest
import asyncio

import polars as pl

from binance_data import BinanceDataDownloader, BinanceDataProvider
from datetime import datetime

class TestBinanceDataDownloader(unittest.TestCase):
    def test_downloader_one_ticker(self):
        downloader = BinanceDataDownloader()
        data = asyncio.run(downloader.download_one_ticker("BTCUSDT", datetime(2020,1,1), datetime(2020,1,31),"1d"))
        print(data.shape)
        self.assertTrue(type(data) == pl.DataFrame and data.shape[0] > 0)

    def test_downloader_one_ticker_invalid(self):
        downloader = BinanceDataDownloader()
        with self.assertRaises(Exception):
            data = asyncio.run(downloader.download_one_ticker("INVALID", datetime(2020,1,1), datetime(2020,1,31),"1w"))

    def test_binance_data_provider(self):
        provider = BinanceDataProvider(["ETHUSDT"], ["1m"])
        asyncio.run(provider.update_tickers(["ETHUSDT"], ["1m"]))
        self.assertTrue(os.path.exists("./tickers/ETHUSDT-1m.csv"))

    # def test_binance_data_provider_ccxt(self):
    #     provider = BinanceDataProvider(["RUNE/USDT:USDT"], ["1m"])
    #     asyncio.run(provider.update_tickers(["RUNE/USDT:USDT"], ["1m"]))
    #     self.assertTrue(os.path.exists("./tickers/RUNEUSDT-1m.csv"))




if __name__ == '__main__':
    unittest.main()