import json

from nltk.sentiment import SentimentIntensityAnalyzer
import pandas as pd
import requests


class VaderSentiment:
    def __init__(self):
        #  nltk.download('vader_lexicon')
        self.sid = SentimentIntensityAnalyzer()

    def get_sentiment(self, filename='../output/text_data_out.csv', dataframe=None, show_text=False):
        if dataframe is not None:
            df_text_data = dataframe
        else:
            df_text_data = pd.read_csv(filename)

        def sentiment(row):
            sentence = row['text']
            id = row['id']
            type = row['type']
            ss = self.sid.polarity_scores(sentence)
            neg = ss['neg']
            neu = ss['neu']
            pos = ss['pos']
            compound = ss['compound']

            if show_text:
                return pd.Series([id, type, sentence, neg, neu, pos, compound],
                             index=['id', 'type', 'text', 'negative', 'neutral', 'positive', 'compound'])

            return pd.Series([id, type, neg, neu, pos, compound],
                             index=['id', 'type', 'negative', 'neutral', 'positive', 'compound'])

        df_sentiment_data = df_text_data.apply(sentiment, axis=1)

        return df_sentiment_data


class AwsComprehendSentiment:
    def __init__(self):
        self.url = 'https://lrs5sbm1n0.execute-api.us-east-1.amazonaws.com/default/comprehend_call/sentiments'

    def get_sentiment(self, filename='../output/text_data_out.csv', dataframe=None, show_text=False):
        if dataframe is not None:
            df_text_data = dataframe
        else:
            df_text_data = pd.read_csv(filename)

        def sentiment(row):
            sentence = row['text']
            payload = json.dumps({'body': sentence})
            try:
                resp = requests.post(self.url,
                                     payload)
                ss = resp.json()['body']['SentimentScore']
                neg = ss['Negative']
                neu = ss['Neutral']
                pos = ss['Positive']
                compound = ss['Mixed']

                id = row['id']
                type = row['type']
                if show_text:
                    return pd.Series([id, type, sentence, neg, neu, pos, compound],
                                     index=['id', 'type', 'text', 'negative', 'neutral', 'positive', 'compound'])

                return pd.Series([id, type, neg, neu, pos, compound],
                                 index=['id', 'type', 'negative', 'neutral', 'positive', 'compound'])
            except Exception as ex:
                print(f'Exception {str(ex)}')

        df_sentiment_data = df_text_data.apply(sentiment, axis=1)

        return df_sentiment_data


if __name__=="__main__":
    comprehend = AwsComprehendSentiment()
    sample = comprehend.get_sentiment()
