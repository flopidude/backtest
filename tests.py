import os
import unittest
import asyncio

import polars as pl
from datetime import datetime
from shutil import rmtree


class TestBinanceDataDownloader(unittest.TestCase):
    def test_downloader_one_ticker(self):
        from historical_binance import BinanceDataDownloader
        downloader = BinanceDataDownloader()
        data = asyncio.run(downloader.download_one_ticker("BTCUSDT", datetime(2020, 1, 1), datetime(2024, 3, 10), "1d"))
        self.assertTrue(type(data) == pl.DataFrame and data.shape[0] > 0)

    def test_downloader_one_ticker_invalid(self):
        from historical_binance import BinanceDataDownloader
        downloader = BinanceDataDownloader()
        with self.assertRaises(Exception):
            data = asyncio.run(
                downloader.download_one_ticker("INVALID", datetime(2020, 1, 1), datetime(2020, 1, 31), "1w"))

    def test_binance_data_provider(self):
        from historical_binance import BinanceDataProvider
        provider = BinanceDataProvider(["ETHUSDT", "BTC/USDT:USDT", "RUNE/USDT:USDT"], ["1m"])
        asyncio.run(provider.update_tickers(["ETHUSDT", "BTC/USDT:USDT"], ["1m"]))
        self.assertTrue(os.path.exists("./tickers/ETHUSDT-1m.feather"))
        rmtree("./tickers")

    def test_binance_data_provider_async(self):
        from historical_binance import BinanceDataProvider
        provider = BinanceDataProvider(["ETHUSDT", "BTC/USDT:USDT"], ["1m"])
        asyncio.run(provider.update_tickers_async(["ETHUSDT", "BTC/USDT:USDT"], ["1m"], datetime(2023, 2, 1)))
        # print(provider.cached_dataframes["1m"]["ETHUSDT"])
        self.assertTrue(os.path.exists("./tickers/ETHUSDT-1m.feather"))
        rmtree("./tickers")

    def test_binance_data_provider_async_big(self):
        from historical_binance import BinanceDataProvider
        provider = BinanceDataProvider(["ETH/USDT:USDT", "BTC/USDT:USDT", "FET/USDT:USDT", "RUNE/USDT:USDT", "SOL/USDT:USDT", "OP/USDT:USDT"], ["1m"])
        asyncio.run(provider.update_tickers_async(["ETH/USDT:USDT", "BTC/USDT:USDT", "FET/USDT:USDT", "RUNE/USDT:USDT", "SOL/USDT:USDT", "OP/USDT:USDT"], ["1m"], datetime(2023, 2, 1)))
        # print(provider.cached_dataframes["1m"]["ETHUSDT"])
        self.assertTrue(os.path.exists("./tickers/ETHUSDT-1m.feather"))
        rmtree("./tickers")

    def test_binance_data_provider_naming_convention(self):
        from historical_binance import BinanceDataProvider
        TEST_MIN = datetime(2023, 12, 7)
        provider = BinanceDataProvider(["ZETA/USDT:USDT", "AGIX/USDT:USDT", "RUNE/USDT:USDT"], ["1m"], "./data/futures",
                                       "{currency}_USDT_USDT-{timeframe}.feather")
        asyncio.run(provider.update_tickers_async(["ZETA/USDT:USDT", "AGIX/USDT:USDT", "RUNE/USDT:USDT"], ["1m"], TEST_MIN))
        print(provider.cached_dataframes["1m"]["ZETA/USDT:USDT"])
        print(provider.cached_dataframes["1m"]["AGIX/USDT:USDT"])
        # print(provider.cached_dataframes["1m"]["ETHUSDT"])
        # self.assertTrue(os.path.exists("./data/futures/ETH_USDT_USDT-1m.feather"))
        # self.assertTrue(os.path.exists("./data/futures/BTC_USDT_USDT-1m.feather"))
        rmtree("./data")


if __name__ == '__main__':
    unittest.main()
