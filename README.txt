Trading Social Sentiment Analysis

DESCRIPTION
This project's goal is to collect data from social forums like Reddit’s r/WallStreetBets and extract sentiment on stocks of interest to predict a bearish or bullish price movement for the stock.

INSTALLATION
Use the package manager pip to install. A list of libraries & packages needed is in the file requirements.txt

pip install -r requirements.txt
EXECUTION
1. Data Scraping & Cleaning
Python modules in the scrapers folder can be run to get the data.

get_file_from_s3.py
google_trend_scraper.py
scraper.py
To build our dataset we setup them as scheduled jobs on AWS Lambda which stores the data on S3

2. Data Analysis
Python modules in the analysis_until folder are used to transform, analyse the cleaned data.

3. Data Storage
The python module analysis_util/combine_data_files.py combines and stores the data

4. Data Visualisation
Finally the Jupyter notebooks under the notebooks folder are used to transform the data and build interactive visualisations

DEMO VIDEO
Submitted for CSE 6242 by team 22 ( ccreighton8, iselmi3, jsung63, mnadjar3, nlisichenok3, pahooja3 )