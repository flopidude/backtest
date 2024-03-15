import asyncio
import httpx
import tqdm
import zipfile
from io import BytesIO
from datetime import datetime, timedelta, date
import polars as pl
from dateutil.relativedelta import relativedelta
import os
from pytz import UTC


class BinanceDataProvider:
    TICKER_DIR = None
    TICKER_NAME = None

    data_downloader = None

    def fetch_dataframe_constraints(self, ticker, timeframe):
        return self.cached_dataframes[timeframe][ticker]['date'].min(), self.cached_dataframes[timeframe][ticker][
            'date'].max()

    async def load_tickers(self):
        """
        This function loads the ticker data from the disk into the memory by using the self.pairlist and self.timeframes.
        It creates the directory for the all the tickers in self.cached_dataframes -> {timeframe: {pair: dataframe}}
        If the data is not available in the disk, it sets the data as None in the dictionary

        To add a new ticker:
        self.pairlist.append(ticker)
        self.load_tickers()
        """
        if not os.path.exists(self.TICKER_DIR):
            os.makedirs(self.TICKER_DIR)
        for timeframe in self.timeframes:
            if timeframe not in self.cached_dataframes.keys():
                self.cached_dataframes[timeframe] = {}
            for pair in self.pairlist:
                if pair in list(self.cached_dataframes[timeframe].keys()):
                    print(f"Skipping existing data for {pair} on {timeframe} timeframe.")
                    continue
                ticker = pair.replace("/USDT:USDT", "USDT")
                basecur = pair.split("/")[0].replace("USDT", "")
                ticker_path = self.TICKER_NAME.format(currency=basecur, timeframe=timeframe, ticker=ticker)

                try:
                    print(f"Loading existing data for {pair} on {timeframe} timeframe.")
                    self.cached_dataframes[timeframe][pair] = pl.read_csv(ticker_path,
                                                                          try_parse_dates=True).with_columns(
                        pl.col("date").cast(pl.Datetime(time_unit="ms", time_zone="UTC")))
                except Exception as e:
                    self.cached_dataframes[timeframe][pair] = None
                    print(f"Error loading existing data for {pair}: {e}")

    async def update_tickers(self, pairs: [str], timeframes: [str], fallback_starting_date: datetime = None):
        """
        This function downloads the new data for the given pairs and timeframes and merges it with the existing data.
        If there is completely no data available, it downloads the data since the fallback_starting_date.
        """
        to_datetime = datetime.combine(date.today(), datetime.min.time())
        if fallback_starting_date is None:
            fallback_starting_date = datetime.combine(date.today() - relativedelta(years=2), datetime.min.time())
        for pair in pairs:
            ticker = pair.replace("/USDT:USDT", "USDT")
            for timeframe in timeframes:
                basecur = pair.split("/")[0].replace("USDT", "")
                ticker_path = self.TICKER_NAME.format(currency=basecur, ticker=ticker, timeframe=timeframe)
                # Get the last available date
                if self.cached_dataframes[timeframe][pair] is not None and not self.cached_dataframes[timeframe][
                    pair].is_empty():
                    _, last_date = self.fetch_dataframe_constraints(pair, timeframe)
                else:
                    last_date = fallback_starting_date

                # Download new data and merge with existing data
                new_data = await self.data_downloader.download_one_ticker(ticker, last_date, to_datetime, timeframe)
                if new_data is not None and not new_data.is_empty():
                    if self.cached_dataframes[timeframe][pair] is not None and not self.cached_dataframes[timeframe][
                        pair].is_empty():
                        self.cached_dataframes[timeframe][pair] = pl.concat(
                            [self.cached_dataframes[timeframe][pair], new_data], how="vertical").unique(
                            subset=["date"]).sort("date")
                    else:
                        self.cached_dataframes[timeframe][pair] = new_data
                    self.cached_dataframes[timeframe][pair].write_csv(ticker_path)
                    print(f"Updated data for {pair}")
                else:
                    print(f"No new data available for {pair}")

    async def update_tickers_async(self, pairs: [str], timeframes: [str], fallback_starting_date:datetime=None):
        """
        This function downloads the new data for the given pairs and timeframes and merges it with the existing data.
        If there is completely no data available, it downloads the data since the fallback_starting_date.
        This is the asynchronous version of the update_tickers function, which runs slightly faster but has issues with rendering the progress bar.
        """
        today_datetime = datetime.combine(date.today(), datetime.min.time())
        if fallback_starting_date is None:
            fallback_starting_date = datetime.combine(date.today() - relativedelta(years=2), datetime.min.time())
        tasks = []
        properties = []
        for timeframe in timeframes:
            for pair in pairs:
                ticker = pair.replace("/USDT:USDT", "USDT")

                # Get the last available date
                if self.cached_dataframes[timeframe][pair] is not None and not self.cached_dataframes[timeframe][
                    pair].is_empty():
                    _, last_date = self.fetch_dataframe_constraints(pair, timeframe)
                else:
                    last_date = fallback_starting_date

                # Download new data and merge with existing data
                task = (asyncio.create_task(
                    self.data_downloader.download_one_ticker(ticker, last_date, today_datetime, timeframe)))
                tasks.append(task)
                properties.append((pair, timeframe))
        tasks = asyncio.as_completed(tasks)
        for coroutine, (pair, timeframe) in zip(tasks, properties):
            new_data = await coroutine
            ticker = pair.replace("/USDT:USDT", "USDT")
            basecur = pair.split("/")[0].replace("USDT", "")
            ticker_path = self.TICKER_NAME.format(currency=basecur, ticker=ticker, timeframe=timeframe)
            if new_data is not None and not new_data.is_empty():
                if self.cached_dataframes[timeframe][pair] is not None and not self.cached_dataframes[timeframe][
                    pair].is_empty():
                    self.cached_dataframes[timeframe][pair] = pl.concat(
                        [self.cached_dataframes[timeframe][pair], new_data], how="vertical").unique(
                        subset=["date"]).sort("date")
                else:
                    self.cached_dataframes[timeframe][pair] = new_data
                self.cached_dataframes[timeframe][pair].write_csv(ticker_path)
                print(f"Updated data for {pair}")
            else:
                print(f"No new data available for {pair}")

    def __init__(self, pairlist: [str], timeframes: [str], ticker_path: str = "./tickers",
                 naming_convention: str = "{ticker}-{timeframe}.csv"):
        self.TICKER_DIR = os.path.realpath(ticker_path)
        self.TICKER_NAME = os.path.join(self.TICKER_DIR, naming_convention)
        self.data_downloader = BinanceDataDownloader()
        self.pairlist = pairlist
        self.timeframes = timeframes
        self.cached_dataframes = {}
        asyncio.run(self.load_tickers())


class BinanceDataDownloader:
    downloadable_ticker_information = {}
    pbar = None
    use_pbar = True
    __minimum_achieved_date = None

    async def download_and_process(self, session, url: str, ticker: str, date_of_cycle: datetime, is_csv=True):
        DEFAULT_COLUMNS = ['open_time', 'open', 'high', 'low', 'close', 'volume',
                           'close_time', 'quote_volume', 'count', 'taker_buy_volume',
                           'taker_buy_quote_volume', 'ignore']
        try:
            response = await session.get(url)
            if response.status_code == 200:
                if is_csv:
                    data = response.read()
                    with zipfile.ZipFile(BytesIO(data)) as zip_file:
                        with zip_file.open(zip_file.namelist()[0]) as csv_file:
                            first_line = csv_file.readline().decode("utf-8").strip()
                            csv_file.seek(0)  # Reset the file pointer to the beginning

                            if "open_time" in first_line:
                                df = pl.read_csv(csv_file.read())
                            else:
                                df = pl.read_csv(csv_file.read(), has_header=False, new_columns=DEFAULT_COLUMNS)
                else:
                    data = response.json()
                    df = pl.DataFrame(data)
                    df.columns = DEFAULT_COLUMNS
                df = df.with_columns((pl.col(coln).cast(pl.Float64) for coln in DEFAULT_COLUMNS if
                                      not coln in ["open_time", "close_time"]))
                df = df.filter(pl.col("ignore") == 0).rename({"open_time": "date"}).drop(
                    ["close_time", "ignore", "quote_volume", "taker_buy_quote_volume"]).with_columns(
                    (pl.col("date").cast(pl.Datetime(time_unit="ms")).dt.replace_time_zone("UTC").alias("date")))
                # print(df)
                if self.pbar is not None:
                    self.pbar.update(1)
                if self.__minimum_achieved_date is None or date_of_cycle < self.__minimum_achieved_date:
                    self.__minimum_achieved_date = date_of_cycle
                return df
            else:
                raise ConnectionError(response.status_code)
        except Exception as e:

            if self.__minimum_achieved_date is not None and (
            not date.today() == date_of_cycle.date()) and self.__minimum_achieved_date < date_of_cycle:
                print(date.today(), date_of_cycle)
                raise Exception(
                    f"A hole in the cycle has been found, minimum achieved date is {self.__minimum_achieved_date}, url: {url.split('/klines')[1]}, {e}")
            if self.pbar is not None:
                self.pbar.write(f"Error: {url.split('/klines')[1]}, {e}")
                self.pbar.update(1)
                if not hasattr(self.pbar, "error_count"):
                    self.pbar.error_count = 1
                else:
                    self.pbar.error_count += 1
                self.pbar.set_postfix_str(f"ErrCount: {self.pbar.error_count}")
                # self.pbar.
            else:
                print(f"Error: Failed to download data for {ticker} from {url}")
            return pl.DataFrame()

    async def download_one_ticker(self, ticker, start_date: datetime, end_date: datetime, timeframe, spot=False):
        # Write some code to ensure that start_date is smaller than end_date and make them both timezone-aware using UTC
        start_date = start_date.replace(tzinfo=UTC)
        end_date = end_date.replace(tzinfo=UTC)
        end_date_plus_one = end_date + timedelta(days=1)

        if start_date > end_date_plus_one:
            raise ValueError(f"start_date({start_date}) must be smaller than end_date({end_date})")

        prefix = "spot" if spot else "futures/um"
        self.__minimum_achieved_date = None
        if ticker not in self.downloadable_ticker_information["symbolList"]:
            raise Exception(f"Ticker {ticker} is not downloadable")
        async with httpx.AsyncClient() as session:
            tasks = []
            current_date = start_date

            while current_date < end_date_plus_one:
                year, month, day = current_date.year, current_date.month, current_date.day
                if current_date.date() >= date.today() - timedelta(days=1):
                    start_time = current_date.timestamp() * 1000
                    end_time = (current_date + timedelta(minutes=1000)).timestamp() * 1000
                    if spot:
                        url = f"https://data-api.binance.vision/api/v3/klines?symbol={ticker}&interval={timeframe}&limit=1000&startTime={start_time}&endTime={end_time}"
                    else:
                        url = f"https://fapi.binance.com/fapi/v1/klines?symbol={ticker}&interval={timeframe}&limit=1000&startTime={start_time}&endTime={end_time}"
                    task = asyncio.create_task(self.download_and_process(session, url, ticker, current_date, False))
                    tasks.append(task)
                    current_date += timedelta(minutes=1000)
                elif current_date.month == end_date.month and current_date.year == end_date.year:
                    # Download daily data for the ending month
                    url = f"https://data.binance.vision/data/{prefix}/daily/klines/{ticker}/{timeframe}/{ticker}-{timeframe}-{year}-{month:02d}-{day:02d}.zip"
                    task = asyncio.create_task(self.download_and_process(session, url, ticker, current_date, True))
                    tasks.append(task)
                    current_date += timedelta(days=1)

                else:
                    # Download monthly data for full months
                    url = f"https://data.binance.vision/data/{prefix}/monthly/klines/{ticker}/{timeframe}/{ticker}-{timeframe}-{year}-{month:02d}.zip"
                    task = asyncio.create_task(self.download_and_process(session, url, ticker, current_date, True))
                    tasks.append(task)
                    # add one month to the current date setting the day to be the first day of the month
                    current_date = current_date.replace(day=1) + relativedelta(months=1)
            if self.use_pbar:
                self.pbar = tqdm.tqdm(total=len(tasks),
                                      desc=f'DOWN {ticker}-{timeframe}, {start_date.date()} to {end_date.date()}')

            dfs = await asyncio.gather(*tasks)
            combined_df = pl.concat([i for i in dfs if i.height > 0], how="vertical")
            if combined_df.shape[0] == 0:
                raise Exception(f"No data found for {ticker} between {start_date} and {end_date}")
            if self.use_pbar:
                self.pbar.close()
            return combined_df

    async def __fetch_downloadable_tickers(self):
        async with httpx.AsyncClient() as session:
            headers = {
                'authority': 'www.binance.com',
                'accept': '*/*',
                'accept-language': 'en-US,en;q=0.9',
                'clienttype': 'web',
                'content-type': 'application/json',
                'dnt': '1',
                'lang': 'en',
                'origin': 'https://www.binance.com',
                'referer': 'https://www.binance.com/en/landing/data',
                'sec-ch-ua': '"Not(A:Brand";v="24", "Chromium";v="122"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            }
            data = {'bizType': 'FUTURES_UM', 'productId': 1}
            response = await session.post(
                'https://www.binance.com/bapi/bigdata/v1/public/bigdata/finance/exchange/listDownloadOptions',
                headers=headers, json=data)
            result = response.json()
            if result["code"] != '000000' or not result["success"]:
                raise Exception(f"Failed to fetch downloadable tickers, {result}")
            return result["data"]

    def __init__(self, use_pbar=True):
        self.use_pbar = use_pbar
        self.downloadable_ticker_information = (asyncio.run(self.__fetch_downloadable_tickers()))
