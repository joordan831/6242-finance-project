import json
from datetime import datetime

import boto3
from pytrends.request import TrendReq
import requests
import pandas as pd
import re
import logging


class NasdaqScraper:
    def __init__(self):
        self.nasdaq_screener_url = 'https://api.nasdaq.com/api/screener/stocks?tableonly=true&offset=0&download=true'

    def download_stock_data(self):
        headers = { "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36" }
        cookies = { "Cookie1": "Value1"}
        try:
            resp = requests.get(self.nasdaq_screener_url, headers=headers, cookies=cookies)
            data = resp.json()
            stock_data = data['data']['rows']

            df_stocks = pd.DataFrame.from_dict(stock_data)
            df_stocks = df_stocks[['symbol', 'name']]
            df_stocks['symbol'] = df_stocks['symbol'].str.lower()
            df_stocks['name'] = df_stocks['name'].str.lower()

            return df_stocks
        except Exception as e:
            logging.error(f'Error downloading data from NASDAQ + {str(e)}')


class GoogleTrendScraper:
    def __init__(self):
        self.pytrends = TrendReq(hl='en-US', tz=360)

    def load_trends(self, keyword="stock", timeframe='now 1-H'):
        kw_list = [keyword]
        self.pytrends.build_payload(kw_list, cat=0, timeframe=timeframe, geo='', gprop='')

    def get_rising_queries(self, keyword="stock"):
        import time
        ts = time.time()
        related_queries = self.pytrends.related_queries()
        df_rising_queries = related_queries[keyword]['rising']
        df_rising_queries['timestamp'] = ts
        df_rising_queries['timestamp_ny'] = pd.Timestamp(ts, unit='s', tz='US/Eastern')
        return df_rising_queries


def scrape_stock_trends():
    try:
        nasdaq_scraper = NasdaqScraper()
        df_stocks = nasdaq_scraper.download_stock_data()
        df_stocks['company_name'] = df_stocks['name'].str.split().str.get(0)

        google_scraper = GoogleTrendScraper()
        google_scraper.load_trends()
        df_rising_stocks = google_scraper.get_rising_queries()

        pattern = re.compile(".*\sstock$")

        df_rising_stocks['stock'] = df_rising_stocks['query'].apply(lambda x: x.split(' stock')[0] if pattern.match(x) else None)
        df_rising_stocks = pd.merge(df_rising_stocks, df_stocks, how="left", left_on="stock", right_on="symbol")
        df_rising_stocks['nasdaq match'] = pd.notna(df_rising_stocks['symbol'])
        return df_rising_stocks
    except Exception as e:
        print(e)


if __name__ == '__main__':
    data = scrape_stock_trends()
    d = data.to_csv()


def lambda_handler(event, context):
    trend_data = scrape_stock_trends()
    now = datetime.utcnow()
    yyyymmdd = now.strftime('%Y%m%d')
    hh = now.strftime('%H')
    bucket_name = "cse-6242-reddit"
    s3_path_1 = "googletrends/" + f"trend_data_{yyyymmdd}_{hh}.csv"

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket_name, Key=s3_path_1, Body=(bytes(json.dumps(trend_data.to_csv(), default=str).encode('UTF-8'))), ACL='public-read')

    return {
        'statusCode': 200
    }
