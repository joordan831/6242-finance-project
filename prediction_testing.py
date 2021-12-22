import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB

from analysis_util.mining import TickersMining
from analysis_util.sentiment import VaderSentiment


def combine_subreddit_texts_by_date(subreddits=['investing', 'stocks', 'wallstreetbets'], date='20211111'):
    dataframe = pd.DataFrame()
    for subreddit in subreddits:
        dataframe = dataframe.append(
            pd.read_csv('notebooks/output/' + subreddit + '/text_data/' + subreddit + '_text_data_' + date + '.csv'),
            ignore_index=True)
    return dataframe


def add_bearish_or_bullish_class(df, stock_data):
    merged = df.merge(stock_data, how='inner', left_on='tickers', right_on='symbol')
    merged['class'] = merged['netchange'].apply(lambda x: 'bull' if x > 0 else 'bear')
    return merged.drop(columns=['symbol', 'lastsale', 'netchange', 'volume', 'marketCap', 'ipoyear', 'industry',
                                'sector', 'url', 'country', 'name', 'pctchange'])


def clean_tickers(sentiment_data):
    to_remove = ["A", "DD", "SE", "TA", "SC"]
    single_tickers = pd.DataFrame(
        columns=['id', 'type', 'text', 'negative', 'neutral', 'positive', 'compound', 'tickers'])

    for a in range(0, len(sentiment_data.index)):
        if isinstance(sentiment_data.iloc[a].loc["tickers"], float):
            pass
        else:
            dict = sentiment_data.iloc[a].to_dict()
            tickers = dict["tickers"].split("|")
            for ticker in to_remove:
                if tickers is None:
                    continue
                elif ticker in tickers:
                    tickers.remove(ticker)
            if len(tickers) > 0:
                for ticker in tickers:
                    dict["tickers"] = ticker
                    single_tickers = single_tickers.append(dict, ignore_index=True)
    return single_tickers


main_frame = pd.DataFrame()
reddit_dates = ['20211111', '20211114', '20211115', '20211116', '20211117', '20211118', '20211121', '20211123',
                '20211124', '20211125', '20211128', '20211129']
nasdaq_dates = ['20211112', '20211115', '20211116', '20211117', '20211118', '20211119', '20211122', '20211124',
                '20211125', '20211126', '20211129', '20211130']
for i in range(len(reddit_dates)):
    all_texts = combine_subreddit_texts_by_date(date=reddit_dates[i])
    vader = VaderSentiment()
    sentiment_data = vader.get_sentiment(dataframe=all_texts, show_text=True)
    miner = TickersMining('notebooks/output/nasdaq/stock_data/nasdaq_stock_data_' + nasdaq_dates[i] + '.csv')

    mined_tickers = miner.get_tickers(dataframe=sentiment_data)
    sentiment_data['tickers'] = mined_tickers['tickers']

    single_tickers = clean_tickers(sentiment_data)
    single_tickers.replace("", float("NaN"), inplace=True)
    single_tickers.dropna(inplace=True)
    grouped = single_tickers.groupby('tickers')
    grouped = grouped.mean()
    grouped['tickers'] = grouped.index
    grouped.index.name = None
    with_class = add_bearish_or_bullish_class(grouped,
                                              pd.read_csv('notebooks/output/nasdaq/stock_data/nasdaq_stock_data_' +
                                                          nasdaq_dates[i] + '.csv'))

    main_frame = main_frame.append(with_class, ignore_index=True)

x = main_frame.drop(columns=['tickers', 'class', 'update_dt'])
y = main_frame.drop(columns=['negative', 'neutral', 'positive', 'compound', 'tickers', 'update_dt'])

# Total records used for training/testing is 2103 from the 12 days worth of data
x = x.to_numpy()
y = y.to_numpy()
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.4, random_state=0)
gnb = GaussianNB()
y_pred = gnb.fit(x_train, y_train).predict(x_test)
counter = 0
for i in range(len(y_pred)):
    if y_pred[i] != y_test[i][0]:
        counter = counter + 1
print("Number of mislabeled points out of a total %d points : %d" % (x_test.shape[0], counter))
# Current output: Number of mislabeled points out of a total 842 points : 365
# Percent of accuracy from testing set: 56.65%
