import json
import logging
import sys
from datetime import datetime, timedelta

import boto3
import pandas
import praw
import requests


# Create a reddit application to access their API using the steps
# https://github.com/reddit-archive/reddit/wiki/OAuth2-Quick-Start-Example#first-steps
from analysis_util.utils import clean_text

REDDIT_SECRET = "Q3IRXdG0wM67KfWd2U7mk63MmigRdg"
REDDIT_CLIENT_ID = "42v-ZFOAoLDfd5uPySJ0bA"
USER_AGENT = "python:dva_team22:v1"
NASDAQ_SCREENER_URL = 'https://api.nasdaq.com/api/screener/stocks?tableonly=true&offset=0&download=true'

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)


class RedditScraper:
    def __init__(self, subreddit="wallstreetbets",
                 sort='new', limit=2500,
                 end_time=datetime.utcnow(),
                 delta_hr=24):
        # Initialize Reddit Praw
        self.reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_SECRET,
            user_agent=USER_AGENT
        )
        # Subreddit name
        self.subreddit = subreddit
        # How we want it to be sorted
        self.sort = sort
        # Number of reddit submissions
        self.limit = limit
        self.end_time = end_time
        self.latest_scraped_data_time = self.end_time
        self.start_time = self.end_time - timedelta(hours=delta_hr)
        self.yyyymmdd = self.end_time.strftime('%Y%m%d')
        logging.info(f'START TIME: {self.start_time.strftime("%Y-%m-%d %H:%M:%S")}')
        logging.info(f'  END TIME: {self.end_time.strftime("%Y-%m-%d %H:%M:%S")}')

    # Set how we want to sort the subReddit results
    def get_subreddit_submissions(self):
        if self.sort == 'new':
            return self.reddit.subreddit(self.subreddit).new(limit=self.limit)
        elif self.sort == 'top':
            return self.reddit.subreddit(self.subreddit).top(limit=self.limit)
        else:
            return self.reddit.subreddit(self.subreddit).hot(limit=self.limit)

    def get_data(self):

        submission_data_dump = []
        comment_data_dump = []
        text_data_dump = []
        author_dump = []
        num_errors = 0
        # iterate through subreddit

        item_count = 1
        for submission in self.get_subreddit_submissions():
            try:
                logging.info(f'--- {item_count} of {self.limit}')
                submission_title = {}
                submission_text = {}
                submission_data = {}
                author_data = {}

                submission_title['id'] = submission.id
                submission_title['type'] = 'title'
                submission_title['text'] = clean_text(submission.title)

                submission_text['id'] = submission.id
                submission_text['type'] = 'submission'
                submission_text['text'] = clean_text(submission.selftext)

                submission_data['submission_id'] = submission.id
                submission_data['author_id'] = submission.author.id

                author_data['author_id'] = submission.author.id
                author_data['name'] = submission.author.name
                author_data['is_gold'] = submission.author.is_gold
                author_data['comment_karma'] = submission.author.comment_karma
                author_data['link_karma'] = submission.author.link_karma
                author_data['awarder_karma'] = submission.author.awarder_karma
                author_data['awardee_karma'] = submission.author.awardee_karma
                author_data['total_karma'] = submission.author.total_karma
                author_data['created_utc'] = datetime.fromtimestamp(submission.author.created_utc)

                # skip if it's too short
                if not (len(submission.selftext) > 10):
                    item_count += 1
                    continue

                submission_data['num_comments'] = submission.num_comments
                submission_data['score'] = submission.score
                submission_data['upvote_ratio'] = submission.upvote_ratio
                submission_data['created_utc'] = datetime.fromtimestamp(submission.created_utc)

                self.latest_scraped_data_time = datetime.fromtimestamp(submission.created_utc)
                logging.info(f'Post time: {self.latest_scraped_data_time.strftime("%Y-%m-%d %H:%M:%S")}')
                if self.latest_scraped_data_time < self.start_time or \
                        self.latest_scraped_data_time > self.end_time:
                    logging.info('Exiting!')
                    break
                # author info
                text_data_dump.append(submission_title)
                text_data_dump.append(submission_text)
                submission_data_dump.append(submission_data)
                author_dump.append(author_data)

                # adding only top-level comment
                for top_level_comment in submission.comments:
                    if isinstance(top_level_comment, praw.models.MoreComments) or len(
                            clean_text(top_level_comment.body)) <= 10:
                        continue
                    comment_id = top_level_comment.id

                    comment_text = {}
                    comment_data = {}
                    author_data = {}

                    comment_text['id'] = comment_id
                    comment_text['type'] = 'comment'
                    comment_text['text'] = clean_text(top_level_comment.body)

                    comment_data['submission_id'] = submission.id
                    comment_data['comment_id'] = comment_id
                    comment_data['author_id'] = top_level_comment.author.id
                    comment_data['score'] = top_level_comment.score
                    comment_data['distinguished'] = top_level_comment.distinguished
                    comment_data['created_utc'] = datetime.fromtimestamp(top_level_comment.created_utc)

                    author_data['author_id'] = top_level_comment.author.id
                    author_data['name'] = top_level_comment.author.name
                    author_data['is_gold'] = top_level_comment.author.is_gold
                    author_data['comment_karma'] = top_level_comment.author.comment_karma
                    author_data['link_karma'] = top_level_comment.author.link_karma
                    author_data['awarder_karma'] = top_level_comment.author.awarder_karma
                    author_data['awardee_karma'] = top_level_comment.author.awardee_karma
                    author_data['total_karma'] = top_level_comment.author.total_karma
                    author_data['created_utc'] = datetime.fromtimestamp(top_level_comment.author.created_utc)

                    text_data_dump.append(comment_text)
                    comment_data_dump.append(comment_data)
                    author_dump.append(author_data)

                item_count += 1
            # Catch & log exceptions
            except Exception as e:
                num_errors += 1
                print("SCRAPING ERROR: " + str(num_errors) + " " + str(e))
        logging.info(f'--- # of posts collected: {len(submission_data_dump)}')
        logging.info(f'--- # of comments collected: {len(comment_data_dump)}')
        logging.info(f'--- last post UTC: {self.latest_scraped_data_time.strftime("%Y-%m-%d %H:%M:%S")}')

        logging.info(f'Total # of posts collected: {len(submission_data_dump)}')
        logging.info(f'Total # of comments collected: {len(comment_data_dump)}')
        logging.info(f'Last post UTC: {self.latest_scraped_data_time.strftime("%Y-%m-%d %H:%M:%S")}')
        return submission_data_dump, text_data_dump, comment_data_dump, author_dump


class NasdaqScraper:
    def __init__(self):
        self.yyyymmdd = datetime.utcnow().strftime('%Y%m%d')

    @staticmethod
    def download_stock_data():
        headers = { "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36" }
        cookies = { "Cookie1": "Value1"}
        try:
            resp = requests.get(NASDAQ_SCREENER_URL, headers=headers, cookies=cookies)
            data = resp.json()
            return data['data']['rows']
        except Exception as e:
            logging.error(f'Error downloading data from NASDAQ + {str(e)}')


def lambda_handler(event, context):
    delta = 2
    end_date = datetime.utcnow()
    hh = end_date.strftime('%H')
    scraper = RedditScraper(subreddit='wallstreetbets', sort='new', end_time=end_date, delta_hr=int(delta))
    submission_data_dump, text_data_dump, comment_data_dump, author_dump = scraper.get_data()

    bucket_name = "cse-6242-reddit"
    s3_path_1 = "wallstreetbets/" + f"submission_data{scraper.yyyymmdd}_{hh}.csv"
    s3_path_2 = "wallstreetbets/" + f"text_data{scraper.yyyymmdd}_{hh}.csv"
    s3_path_3 = "wallstreetbets/" + f"comment_data{scraper.yyyymmdd}_{hh}.csv"
    s3_path_4 = "wallstreetbets/" + f"author_data{scraper.yyyymmdd}_{hh}.csv"

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket_name, Key=s3_path_1, Body=(bytes(json.dumps(submission_data_dump, default=str).encode('UTF-8'))), ACL='public-read')
    s3.put_object(Bucket=bucket_name, Key=s3_path_2, Body=(bytes(json.dumps(text_data_dump, default=str).encode('UTF-8'))), ACL='public-read')
    s3.put_object(Bucket=bucket_name, Key=s3_path_3, Body=(bytes(json.dumps(comment_data_dump, default=str).encode('UTF-8'))), ACL='public-read')
    resp = s3.put_object(Bucket=bucket_name, Key=s3_path_4, Body=(bytes(json.dumps(author_dump, default=str).encode('UTF-8'))), ACL='public-read')

    if hh == 0:
        nasdaq_scraper = NasdaqScraper()
        stock_data = nasdaq_scraper.download_stock_data()
        s3_path_5 = "nasdaq/" + f"stock_data{scraper.yyyymmdd}.csv"
        s3.put_object(Bucket=bucket_name, Key=s3_path_5,
                      Body=(bytes(json.dumps(stock_data, default=str).encode('UTF-8'))), ACL='public-read')
    return {
        'statusCode': 200
    }


def select_by_date_and_tickers(start_date_time='2021-11-03 14:30:00', end_date_time='2021-11-03 21:00:00',
                               tickers=['DWAC', 'AMC'],
                               input_filename='./24hr_data/submssion_comment_text.csv'):
    dataframe = pandas.read_csv(input_filename)
    return dataframe.loc[(dataframe['created_utc'] > start_date_time) & (dataframe['created_utc'] <= end_date_time) & (
        dataframe['tickers'].isin(tickers))]
