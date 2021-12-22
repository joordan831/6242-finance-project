import os
import sqlite3
import csv

from scrapers.get_file_from_s3 import get_all_files
from analysis_util.mining import TickersMining
from analysis_util.sentiment import VaderSentiment


def drop_table_if_exists(c, conn, tables):
    for table in tables:
        table = table[0]
        drop = "DROP TABLE IF EXISTS "+table
        c.execute(drop)
        conn.commit()

def create_tables(c):
    c.execute('''
        CREATE TABLE IF NOT EXISTS author_data (
            author_id TEXT,
            name TEXT,
            is_gold TEXT,
            comment_karma INTEGER,
            link_karma INTEGER,
            awarder_karma INTEGER,
            awardee_karma INTEGER,
            total_karma INTEGER,
            created_utc TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS comment_data (
            submission_id TEXT,
            comment_id TEXT,
            author_id TEXT,
            score INTEGER, 
            distinguished TEXT,
            created_utc TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS submission_data (
            submission_id TEXT,
            author_id TEXT,
            num_comments INTEGER,
            score INTEGER,
            upvote_ratio REAL,
            created_utc TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS text_data (
            id TEXT,
            type TEXT,
            text TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS sentiment_data (
            _ INTEGER,
            id TEXT,
            type INTEGER,
            negative REAL,
            neutral REAL,
            positive REAL,
            compound REAL,
            tickers TEXT
        )
    ''')

def add_data_to_tables(c, conn, data, data_type):
    if data_type=="author":
        with open(data,'r') as fin:
            dr = csv.DictReader(fin)
            to_db = [(i['author_id'], i['name'], i['is_gold'], i['comment_karma'], i['link_karma'], i['awarder_karma'], i['awardee_karma'], i['total_karma'], i['created_utc']) for i in dr]
        c.executemany('''
            INSERT INTO author_data (
                author_id,
                name,
                is_gold,
                comment_karma,
                link_karma,
                awarder_karma,
                awardee_karma,
                total_karma,
                created_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            ''', to_db)
    elif data_type=="comment":
        with open(data,'r') as fin:
            dr = csv.DictReader(fin)
            to_db = [(i['submission_id'], i['comment_id'], i['author_id'], i['score'], i['distinguished'], i['created_utc']) for i in dr]
        c.executemany('''
            INSERT INTO comment_data (
                submission_id,
                comment_id,
                author_id,
                score, 
                distinguished,
                created_utc
                ) VALUES (?, ?, ?, ?, ?, ?);
            ''', to_db)
    elif data_type=="submission":
        with open(data,'r') as fin:
            dr = csv.DictReader(fin)
            to_db = [(i['submission_id'], i['author_id'], i['num_comments'], i['score'], i['upvote_ratio'], i['created_utc']) for i in dr]
        c.executemany('''
            INSERT INTO submission_data (
                submission_id,
                author_id,
                num_comments,
                score,
                upvote_ratio ,
                created_utc
                ) VALUES (?, ?, ?, ?, ?, ?);
            ''', to_db)
    elif data_type=="text":
        with open(data,'r', encoding="utf8") as fin:
            dr = csv.DictReader(fin)
            to_db = [(i['id'], i['type'], i['text']) for i in dr]
        c.executemany('''
            INSERT INTO text_data (
                id,
                type,
                text
                ) VALUES (?, ?, ?);
            ''', to_db)
    elif data_type=="sentiment":
        with open(data,'r') as fin:
            dr = csv.DictReader(fin)
            to_db = [(i[''], i['id'], i['type'], i['negative'], i['neutral'], i['positive'], i['compound'], i['tickers']) for i in dr]
        c.executemany('''
            INSERT INTO sentiment_data (
                _,
                id,
                type,
                negative,
                neutral,
                positive,
                compound,
                tickers
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            ''', to_db)
    conn.commit()

def create_full_data():
    # combine comment_data, text_data, and sentiment_data
    c.execute('''
        CREATE TABLE comment_text_data AS
            SELECT *
            FROM comment_data
            JOIN text_data
            ON comment_data.comment_id = text_data.id
    ''')
    c.execute('''
        CREATE TABLE comment_text_sentiment_data AS
            SELECT 
                ctd.submission_id,
                ctd.comment_id,
                ctd.author_id,
                ctd.score,
                ctd.distinguished,
                ctd.created_utc,
                ctd.type,
                ctd.text,
                sd.negative,
                sd.neutral,
                sd.positive,
                sd.compound,
                sd.tickers
            FROM comment_text_data ctd
            JOIN sentiment_data sd
            ON ctd.comment_id = sd.id
    ''')
    c.execute("DROP TABLE IF EXISTS comment_text_data")
    conn.commit()

    # combine submission_data, text_data, and sentiment_data
    c.execute('''
        CREATE TABLE submission_text_data AS
            SELECT *
            FROM submission_data
            JOIN text_data
            ON submission_data.submission_id = text_data.id
    ''')
    c.execute('''
        CREATE TABLE submission_text_sentiment_data AS
            SELECT 
                std.submission_id,
                std.author_id,
                std.num_comments,
                std.score,
                std.upvote_ratio,
                std.created_utc,
                std.type,
                std.text,
                sd.negative,
                sd.neutral,
                sd.positive,
                sd.compound,
                sd.tickers
            FROM submission_text_data std
            JOIN sentiment_data sd
            WHERE std.submission_id = sd.id
            AND std.type = sd.type
    ''')
    c.execute("DROP TABLE IF EXISTS submission_text_data")
    conn.commit()

    # combine submission_text_sentiment_data and comment_text_sentiment_data
    c.execute('''
        CREATE TABLE comment_submission_text_sentiment_data (
            submission_id TEXT,
            comment_id TEXT,
            author_id TEXT,
            score INTEGER, 
            num_comments INTEGER,
            upvote_ratio REAL,
            distinguished TEXT,
            created_utc TEXT,
            type TEXT,
            text TEXT,
            negative REAL,
            neutral REAL,
            positive REAL,
            compound REAL,
            tickers TEXT
        )
    ''')
    c.execute('''
        INSERT INTO comment_submission_text_sentiment_data (
                submission_id,
                comment_id,
                author_id,
                score, 
                distinguished,
                created_utc,
                type,
                text,
                negative,
                neutral,
                positive,
                compound,
                tickers
            )
            SELECT
                submission_id,
                comment_id,
                author_id,
                score, 
                distinguished,
                created_utc,
                type,
                text,
                negative,
                neutral,
                positive,
                compound,
                tickers
            FROM comment_text_sentiment_data
    ''')
    c.execute("DROP TABLE IF EXISTS comment_text_sentiment_data")
    conn.commit()

    c.execute('''
        INSERT INTO comment_submission_text_sentiment_data (
                submission_id,
                author_id,
                num_comments,
                upvote_ratio,
                score, 
                created_utc,
                type,
                text,
                negative,
                neutral,
                positive,
                compound,
                tickers
            )
            SELECT
                submission_id,
                author_id,
                num_comments,
                upvote_ratio,
                score, 
                created_utc,
                type,
                text,
                negative,
                neutral,
                positive,
                compound,
                tickers
            FROM submission_text_sentiment_data
    ''')
    c.execute("DROP TABLE IF EXISTS submission_text_sentiment_data")
    conn.commit()

    # add author_data to create full_data
    # c.execute('''
    #     CREATE TABLE full_data AS
    #         SELECT 
    #             cstsd.submission_id,
    #             cstsd.comment_id,
    #             cstsd.author_id,
    #             cstsd.num_comments,
    #             cstsd.score,
    #             cstsd.upvote_ratio,
    #             cstsd.distinguished,
    #             cstsd.created_utc,
    #             ad.name,
    #             ad.is_gold,
    #             ad.comment_karma,
    #             ad.link_karma,
    #             ad.awarder_karma,
    #             ad.awardee_karma,
    #             ad.total_karma,
    #             cstsd.type,
    #             cstsd.text,
    #             cstsd.negative,
    #             cstsd.neutral,
    #             cstsd.positive,
    #             cstsd.compound,
    #             cstsd.tickers
    #         FROM comment_submission_text_sentiment_data cstsd
    #         JOIN author_data ad
    #         WHERE cstsd.author_id = ad.author_id
    #         AND cstsd.created_utc = ad.created_utc
    # ''')
    # c.execute("DROP TABLE IF EXISTS comment_submission_text_sentiment_data")
    # conn.commit()


if __name__=="__main__":
    # Step 1: pull files from AWS
    # TODO: need to only pull NEW files (if file exists: nothing, else: download)
    # TODO: when I ran it, I only for files from wallstreetbets
    # TODO: noticed each s3 csv file has all previous days (aka 20211113 also contains 12 and 11), would be better if each time s3 scrapped, it just added the new data to the end of the current file that way, when we pull from s3 we only pull 1 submission file, 1 author file, etc
    get_all_files()

    # Step 2: get sentiment for each text file
    print("STEP 2: creating sentiment_data")
    vader = VaderSentiment()
    miner = TickersMining()
    files = os.listdir("./output")
    for f in files:
        if "text" in f:
            text_data = "./output/"+f
            date = text_data[-12:-4]
            sentiment_data = "./output/sentiment_data"+str(date)+".csv"
            if not os.path.exists(sentiment_data):
                df_sentiment_data = vader.get_sentiment(filename=text_data)
                df_ticker_data = miner.get_tickers(text_data=text_data)
                df_sentiment_data['tickers'] = df_ticker_data['tickers']
                df_sentiment_data.to_csv(sentiment_data)

    # Step 3: create sqlite db
    print("STEP 3: creating and connecting db")
    db = "./output/data.db"
    conn = sqlite3.connect(db)
    c = conn.cursor()
    tables = c.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    drop_table_if_exists(c, conn, tables)
    create_tables(c)
    for f in files:
        if date in f:
            data = "./output/"+f
            if "author" in f:
                add_data_to_tables(c, conn, data, "author")
            elif "comment" in f:
                add_data_to_tables(c, conn, data, "comment")
            elif "submission" in f:
                add_data_to_tables(c, conn, data, "submission")
            elif "text" in f:
                add_data_to_tables(c, conn, data, "text")
            elif "sentiment" in f:
                add_data_to_tables(c, conn, data, "sentiment")
    
    # Step 4: combine tables in db into a single DB table called full_data
    # need submission_id attached to author_data to correctly join author data with full_data
    print("STEP 4: creating full_data table")
    create_full_data()

    # TODO: seperate tickers so each line contains only 1 ticker
    # TODO: extend title/submission tickers to comments (if title ticker is TSLA we can assume that comments are also talking about TSLA)

    conn.close()