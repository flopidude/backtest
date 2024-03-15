# A very simple fast fetcher for Binance historical market data.

#### By using a combination of historical CSV files and API calls this tiny library can avoid rate limits and fetch data significantly faster than alternatives that fetch just from the API.

here is a benchmark for you :)

| Downloaded tickers                                                                       | Time range        | **HistoricalBinance** | **Freqtrade** |
|------------------------------------------------------------------------------------------|-------------------|-----------------------|---------------|
| BTC/USDT:USDT, ETH/USDT:USDT                                                             | 20230201-20240315 | 5.139s                | 5min 11.78s   |
| BTC/USDT:USDT, ETH/USDT:USDT, OP/USDT:USDT, FET/USDT:USDT, SOL/USDT:USDT, RUNE/USDT:USDT | 20230201-20240315 | 9.68s                 | 14min 28.22s  |

### Usage
##### The library includes two classes - BinanceDataDownloader and BinanceDataDownloader

BinanceDataDownloader is a very simple class that just fetches the available tickers to download and upon requesting the `download_one_ticker(ticker, start_date, end_date, timeframe)` function will return a **polars** dataframe requested. If necessary it can be easily converted to pandas.

```
 downloader = BinanceDataDownloader()
 data = await downloader.download_one_ticker("BTCUSDT", datetime(2020, 1, 1), datetime(2024, 3, 10), "1d")
```

BinanceDataProvider is served as an example of how to effectively use the library - caching and asynchronously fetching multiple tickers.

Here is an example of how to use BinanceDataProvider:

```
from binance_data import BinanceDataProvider 
 
async def binance_data_provider_example(self):
    provider = BinanceDataProvider(["ETH/USDT:USDT", "BTC/USDT:USDT", "FET/USDT:USDT", "RUNE/USDT:USDT", "SOL/USDT:USDT", "OP/USDT:USDT"], ["1m"])
    await provider.update_tickers_async(["ETH/USDT:USDT", "BTC/USDT:USDT", "FET/USDT:USDT", "RUNE/USDT:USDT", "SOL/USDT:USDT", "OP/USDT:USDT"], ["1m"], datetime(2023, 2, 1))
    print(provider.cached_dataframes["1m"]["ETHUSDT"])
```
This will print out a **polars** dataframe with columns 
`'date', 'open', 'high', 'low', 'close', 'volume',
                           'count', 'taker_buy_volume'`, however if you wish you can modify the code to keep the 'ignore', 'taker_buy_quote_volume' and 'quote_volume' fetched from Binance.