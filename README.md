## Stock data scraper and Google Sheet watchlist updater

This project was created to scrape multiple website data sources to obtain stock fundamental and technical data. Once the code has scraped all the data it inserts it into a Google sheet. You can then use filters on columns to rank stocks by various criteria or create custom scoring systems. Some examples of scoring systems are given in the sheet under the `SCORES` and `DuckmanTML` sheets. 

[Click here to go to my Google Sheet to view a watchlist example](https://docs.google.com/spreadsheets/d/1pwp7VYWxzv7FFONqofKytrpZMYkKsgms-KezkQHmfBk/edit?usp=sharing) filled with data.

**NOTE: I will not be updating this code. You may find some code snippets useful for your own projects.**

### Websites scraped

* WhaleWisdom
* OpenInsider
* Finviz
* Yahoo Finance

### Setup

I followed [this tutorial](https://pyshark.com/google-sheets-api-using-python/) to create the API credentials (`gsheets-py.json`) that are required in the working directory to access your Google sheet and to set enable the python API on Google Cloud Platform. The tutorial also gives examples of getting the sheet URL and how to use the Google Sheet API in python. 

Create a virtual environment `python3 -m venv venv`, enable the environment `source venv/bin/activate`. Install the requirements `pip install -r requirements.txt`. Update Google Sheet share URL in `config.yaml` which is currently set to my publicly accessible URL to show you how I set up my Sheet to hold all the scraped data and to attempt to score and rank stocks. [Click here to go to my Google Sheet to view a watchlist example](https://docs.google.com/spreadsheets/d/1pwp7VYWxzv7FFONqofKytrpZMYkKsgms-KezkQHmfBk/edit?usp=sharing). Use my sheet as a template (or make a copy to your account). Note the name of the main sheet is `Table`. This is set in the code so unless you change the code or name your sheet the same, the code will fail to open the table and be able to read and write contents. 

### Usage

Enter some tickers in the `TICKER` column of the sheet. Run the code `python3 watchlist_update.py` 

### Help output

`python3 watchlist_update.py --help`

```
Watchlist Updater

options:
  -h, --help            show this help message and exit
  --all                 Update all data
  --new                 Update new tickers only
  --basic               Update basic data only
  --eps                 Update EPS data only
  --rev                 Update revenue data only
  --fundamental         Update fundamental & ratio data only
  --insider             Update insider buying
  --whale               Update whale data
  --fromticker FROMTICKER
                        Continue from ticker
```