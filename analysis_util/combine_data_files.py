'''
This is code written to combine the submission, comment, text, and (in the future) author combine_submission_files
-ccreighton8
'''

from nltk import sentiment, text
import pandas as pd
import statistics
import numpy as np
import time


def combine_submission_comment_text_sentiment(output_path, submission_data, comment_data, text_data, sentiment_data):
    df_submission = pd.read_csv(submission_data)
    df_comment = pd.read_csv(comment_data)
    df_text = pd.read_csv(text_data)
    df_sentiment = pd.read_csv(sentiment_data)
    day = str(int(submission_data[-6:-4])-1)
    if len(day)==1:
        day = "0"+day

    df = pd.DataFrame(columns=["submission_id",
                               "comment_id",
                               "author_id",
                               "type",
                               "text",
                               "num_comments",
                               "score",
                               "upvote_ratio",
                               "distinguished",
                               "negative",
                               "neutral",
                               "positive",
                               "compound",
                               "tickers",
                               "created_utc"])

    df_dict_clear = {"submission_id": None,
                     "comment_id": None,
                     "author_id": None,
                     "type": None,
                     "text": None,
                     "num_comments": None,
                     "score": None,
                     "upvote_ratio": None,
                     "distinguished": None,
                     "negative": None,
                     "neutral": None,
                     "positive": None,
                     "compound": None,
                     "tickers": None,
                     "created_utc": None}
    df_dict = df_dict_clear.copy()
    for a in range(0, len(df_submission.index)):
        if day != df_submission.iloc[a].loc["created_utc"][8:10]:
            continue
        df_dict["submission_id"] = df_submission.iloc[a].loc["submission_id"]
        df_dict["author_id"] = df_submission.iloc[a].loc["author_id"]
        df_dict["type"] = "title"
        df_dict["text"] = df_text.loc[(df_text["id"]==df_submission.iloc[a].loc["submission_id"])&(df_text["type"]==df_dict["type"])]["text"].values[0]
        df_dict["num_comments"] = df_submission.iloc[a].loc["num_comments"]
        df_dict["score"] = df_submission.iloc[a].loc["score"]
        df_dict["upvote_ratio"] = df_submission.iloc[a].loc["upvote_ratio"]
        df_dict["negative"] = df_sentiment.loc[(df_sentiment["id"]==df_submission.iloc[a].loc["submission_id"])&(df_sentiment["type"]==df_dict["type"])]["negative"].values[0]
        df_dict["neutral"] = df_sentiment.loc[(df_sentiment["id"]==df_submission.iloc[a].loc["submission_id"])&(df_sentiment["type"]==df_dict["type"])]["neutral"].values[0]
        df_dict["positive"] = df_sentiment.loc[(df_sentiment["id"]==df_submission.iloc[a].loc["submission_id"])&(df_sentiment["type"]==df_dict["type"])]["positive"].values[0]
        df_dict["compound"] = df_sentiment.loc[(df_sentiment["id"]==df_submission.iloc[a].loc["submission_id"])&(df_sentiment["type"]==df_dict["type"])]["compound"].values[0]
        df_dict["tickers"] = df_sentiment.loc[(df_sentiment["id"]==df_submission.iloc[a].loc["submission_id"])&(df_sentiment["type"]==df_dict["type"])]["tickers"].values[0]
        df_dict["created_utc"] = df_submission.iloc[a].loc["created_utc"]
        df = df.append(df_dict, ignore_index=True)

        df_dict["type"] = "submission"
        df_dict["text"] = df_text.loc[(df_text["id"]==df_submission.iloc[a].loc["submission_id"])&(df_text["type"]=="submission")]["text"].values[0]
        df_dict["negative"] = df_sentiment.loc[(df_sentiment["id"]==df_submission.iloc[a].loc["submission_id"])&(df_sentiment["type"]==df_dict["type"])]["negative"].values[0]
        df_dict["neutral"] = df_sentiment.loc[(df_sentiment["id"]==df_submission.iloc[a].loc["submission_id"])&(df_sentiment["type"]==df_dict["type"])]["neutral"].values[0]
        df_dict["positive"] = df_sentiment.loc[(df_sentiment["id"]==df_submission.iloc[a].loc["submission_id"])&(df_sentiment["type"]==df_dict["type"])]["positive"].values[0]
        df_dict["compound"] = df_sentiment.loc[(df_sentiment["id"]==df_submission.iloc[a].loc["submission_id"])&(df_sentiment["type"]==df_dict["type"])]["compound"].values[0]
        df_dict["tickers"] = df_sentiment.loc[(df_sentiment["id"]==df_submission.iloc[a].loc["submission_id"])&(df_sentiment["type"]==df_dict["type"])]["tickers"].values[0]
        df = df.append(df_dict, ignore_index=True)

        df_dict = df_dict_clear.copy()
        for b in range(0, len(df_comment.index)):
            if df_submission.iloc[a].loc["submission_id"] == df_comment.iloc[b].loc["submission_id"]:
                df_dict["submission_id"] = df_submission.iloc[a].loc["submission_id"]
                df_dict["comment_id"] = df_comment.iloc[b].loc["comment_id"]
                df_dict["author_id"] = df_comment.iloc[b].loc["author_id"]
                df_dict["type"] = "comment"
                df_dict["text"] = df_text.loc[(df_text["id"]==df_comment.iloc[b].loc["comment_id"])&(df_text["type"]=="comment")]["text"].values[0]
                df_dict["score"] = df_comment.iloc[b].loc["score"]
                df_dict["negative"] = df_sentiment.loc[(df_sentiment["id"]==df_comment.iloc[b].loc["comment_id"])&(df_sentiment["type"]==df_dict["type"])]["negative"].values[0]
                df_dict["neutral"] = df_sentiment.loc[(df_sentiment["id"]==df_comment.iloc[b].loc["comment_id"])&(df_sentiment["type"]==df_dict["type"])]["neutral"].values[0]
                df_dict["positive"] = df_sentiment.loc[(df_sentiment["id"]==df_comment.iloc[b].loc["comment_id"])&(df_sentiment["type"]==df_dict["type"])]["positive"].values[0]
                df_dict["compound"] = df_sentiment.loc[(df_sentiment["id"]==df_comment.iloc[b].loc["comment_id"])&(df_sentiment["type"]==df_dict["type"])]["compound"].values[0]
                df_dict["tickers"] = df_sentiment.loc[(df_sentiment["id"]==df_comment.iloc[b].loc["comment_id"])&(df_sentiment["type"]==df_dict["type"])]["tickers"].values[0]
                df_dict["distinguished"] = df_comment.iloc[b].loc["distinguished"]
                df_dict["created_utc"] = df_comment.iloc[b].loc["created_utc"]
                df = df.append(df_dict, ignore_index=True)

    df.to_csv(output_path)

def remove_noTicker_rows(output_path, analysis_path, df):
    df_dict_clear = {"submission_id": None,
                     "comment_id": None,
                     "author_id": None,
                     "type": None,
                     "text": None,
                     "num_comments": None,
                     "score": None,
                     "upvote_ratio": None,
                     "distinguished": None,
                     "negative": None,
                     "neutral": None,
                     "positive": None,
                     "compound": None,
                     "tickers": None,
                     "created_utc": None}

    df2 = pd.DataFrame(columns=["submission_id",
                               "comment_id",
                               "author_id",
                               "type",
                               "text",
                               "num_comments",
                               "score",
                               "upvote_ratio",
                               "distinguished",
                               "negative",
                               "neutral",
                               "positive",
                               "compound",
                               "tickers",
                               "created_utc"])
    df_dict = df_dict_clear.copy()
    # to_remove = ["A", "DD", "SE", "TA", "SC"]
    # "DD", "ATH","USA", "SP", "ET", "RSI", "A", "TA"

    # for a in range(0, len(df.index)):
    #     if isinstance(df.iloc[a].loc["tickers"], float):
    #         pass
    #     else:
    #         dict = df.iloc[a].to_dict()
    #         tickers = dict["tickers"].split("|")
    #         for ticker in to_remove:
    #             if tickers is None:
    #                 continue
    #             elif ticker in tickers:
    #                 tickers.remove(ticker)
    #         if len(tickers)>0:
    #             for ticker in tickers:
    #                 dict["tickers"] = ticker
    #                 df2 = df2.append(dict, ignore_index=True)

    analysis(analysis_path, df2)

    df2.to_csv(output_path)

def analysis(output_path, df):
    analysis = {}

    for a in range(0, len(df.index)):
        ticker = df.iloc[a].loc["tickers"]
        if ticker not in analysis:
            analysis[ticker] = {"count": 1, "compounds": [df.iloc[a].loc["compound"]]}
        else:
            analysis[ticker]["count"] += 1
            analysis[ticker]["compounds"].append(df.iloc[a].loc["compound"])


    df = pd.DataFrame(columns=["ticker",
                               "count",
                               "stdev",
                               "avg(compound)"])
    for i, ticker in enumerate(analysis):
        if len(analysis[ticker]["compounds"])>1:
            analysis[ticker]["stdev"] = statistics.stdev(analysis[ticker]["compounds"])
        else:
            analysis[ticker]["stdev"] = 0
        analysis[ticker]["avg(compound)"] = sum(analysis[ticker]["compounds"])/len(analysis[ticker]["compounds"])
        df = df.append(analysis[ticker], ignore_index=True)
        df["ticker"] = df["ticker"].replace([np.nan], ticker)

    df = df.sort_values("count", ascending=False)

    # print(df.head(5))
    df.head(5).to_csv(output_path)    

for day in range(20211107, 20211114+1):
    output_path = "./multi_day_data/output/submission_comment_text"+str(day)+".csv"

    # submission_data = "./multi_day_data/submission_data"+str(day)+".csv"
    # comment_data = "./multi_day_data/comment_data"+str(day)+".csv"
    # text_data = "./multi_day_data/text_data"+str(day)+".csv"

    # vader = VaderSentiment()
    # df_sentiment_data = vader.get_sentiment(filename=text_data)
    # miner = TickersMining()
    # df_ticker_data = miner.get_tickers(text_data=text_data)
    # df_sentiment_data['tickers'] = df_ticker_data['tickers']
    # df_sentiment_data.to_csv("./multi_day_data/sentiment_data"+str(day)+".csv")
    # sentiment_data = "./multi_day_data/sentiment_data"+str(day)+".csv"

    # start_time = time.time()
    # combine_submission_comment_text_sentiment(output_path, submission_data, comment_data, text_data, sentiment_data)
    # end_time = time.time()
    # print("Time:", (end_time-start_time))

    # submission_comment_text_sentiment_df = pd.read_csv(output_path)
    # print("Before:", len(submission_comment_text_sentiment_df.index))
    
    output_path = "./multi_day_data/output/submission_comment_text_tickersONLY"+str(day)+".csv"
    # analysis_path = "./multi_day_data/output/analysis"+output_path[-12:]  
    # remove_noTicker_rows(output_path, analysis_path, submission_comment_text_sentiment_df)

    submission_comment_text_sentiment_tickersONLY_df = pd.read_csv(output_path)
    # print("After:", len(submission_comment_text_sentiment_tickersONLY_df.index))

    rolling_30_path = "./multi_day_data/output/rolling_30"+output_path[-12:]
    if len(submission_comment_text_sentiment_tickersONLY_df.index)>30:
        rolling_30_sample_df = submission_comment_text_sentiment_tickersONLY_df.sample(n=30)
        rolling_30_sample_df = rolling_30_sample_df[["text", "tickers"]]
        print(rolling_30_sample_df)
        rolling_30_sample_df.to_csv(rolling_30_path)    


