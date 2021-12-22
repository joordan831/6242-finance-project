import pandas as pd
import re


class TickersMining:

    def __init__(self, stock_data='output/stock_data.csv'):

        self.df_stock_data = pd.read_csv(stock_data)

        # Remove common Reddit phrases from
        # REF: https://marketrealist.com/p/wallstreetbets-lingo-guide/
        removal_phrases = ["DD", "ATH", "USA", "SP", "ET", "RSI", "A", "TA"]
        tickers = set(self.df_stock_data['symbol'])
        for phrase in removal_phrases:
            tickers.remove(phrase)
        self.tickers = tickers

    def get_tickers_sentence(self, sentence):
        words = sentence.split()
        words = [re.sub(r'[^a-zA-Z0-9]', '', word) for word in words]
        words = set(words) # remove duplicates

        found_tickers = self.tickers.intersection(words)
        found_tickers = list(found_tickers)
        found_tickers = "|".join(found_tickers)
        return found_tickers

    def get_tickers(self, text_data='output/text_data.csv', dataframe=None):
        if dataframe is not None:
            df_text_data = dataframe
        else:
            df_text_data = pd.read_csv(text_data)

        def tickers(row):
            sentence = row['text']
            id = row['id']
            type = row['type']
            tickers = self.get_tickers_sentence(sentence)

            return pd.Series([id, type, tickers],
                             index=['id', 'type', 'tickers'])

        df_ticker_data = df_text_data.apply(tickers, axis=1)

        # df_ticker_data.to_csv('output/ticker_data.csv')
        return df_ticker_data
