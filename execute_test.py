from datetime import datetime

from analysis_util.mining import TickersMining
from analysis_util.sentiment import VaderSentiment
from analysis_util.utils import save_to_csv
from scrapers.scraper import RedditScraper, NasdaqScraper, select_by_date_and_tickers


if __name__ == '__main__':
    # test_reddit()

    delta = 12
    end_date = datetime.utcnow()
    hh = end_date.strftime('%H')
    scraper = RedditScraper(subreddit='wallstreetbets', sort='new', end_time=end_date, delta_hr=int(delta))
    submission_data_dump, text_data_dump, comment_data_dump, author_dump = scraper.get_data()

    save_to_csv(submission_data_dump, 'a', f'output/submission_data{scraper.yyyymmdd}_{hh}.csv')
    save_to_csv(text_data_dump, 'a', f'output/text_data{scraper.yyyymmdd}_{hh}.csv')
    save_to_csv(comment_data_dump, 'a', f'output/comment_data{scraper.yyyymmdd}_{hh}.csv')
    save_to_csv(author_dump, 'a', f'output/author_data{scraper.yyyymmdd}_{hh}.csv')

    nasdaq_scraper = NasdaqScraper()
    stock_data = nasdaq_scraper.download_stock_data()
    save_to_csv(stock_data, 'w', f'output/stock_data{nasdaq_scraper.yyyymmdd}_{hh}.csv')

    vader = VaderSentiment()
    df_sentiment_data = vader.get_sentiment(filename=f'output/text_data{scraper.yyyymmdd}_{hh}.csv')

    miner = TickersMining()
    df_ticker_data = miner.get_tickers(text_data=f'output/text_data{scraper.yyyymmdd}_{hh}.csv')

    df_sentiment_data['tickers'] = df_ticker_data['tickers']

    df_sentiment_data.to_csv(f'output/sentiment_data{scraper.yyyymmdd}_{hh}.csv')

    select_by_date_and_tickers('2021-11-03 14:30:00', '2021-11-03 21:00:00', ['AMC', 'DWAC'],
              './24hr_data/submssion_comment_text.csv').to_csv('./24hr_data/custom_selection.csv')
