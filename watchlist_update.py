import gspread
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np
import requests
from lxml import html
import pandas as pd
import yfinance as yf
from time import sleep
from finvizfinance.quote import finvizfinance
import datetime as dt
import argparse
import sys
from bs4 import BeautifulSoup
import yaml

def get_watchlist_tickers(gsheet_url):
    # set gsheets creds
    gc = gspread.service_account(filename='gsheets-py.json')

    # url of watchlist
    gsheet = gc.open_by_url(gsheet_url)

    # open main table with all data in
    maintable = gsheet.worksheet("Table")

    # get tickers
    tickers = maintable.col_values(1)[2:]
    #print(tickers)

    return maintable, tickers


def check_round(val):

    if isinstance(val, str):
        if len(val) == 0:
            return val
        val = float(val)

    if val is None:
        return ''

    if val == 0:
        return 0.0

    return np.round(val, 2)


def check_nan(val):

    if isinstance(val, str):
        if '%' in val:
            return val
        if 'M' in val:
            return val
        if 'k' in val:
            return str(float(val.strip('k'))/1000.)+'M'
        if 'B' in val:
            return val
        if 'T' in val:
            return val
        if len(val) == 0:
            return val
        if val == '-':
            return ''
        if val == 'N/A':
            return ''
        if ',' in val:
            return val
        val = float(val)

    if np.isnan(val):
        return ''

    return str(val)


def unit_convert(val):

    if isinstance(val, str):
        # convert B to M
        if 'B' in val:
            return float(val.strip('B'))*1e3
        # convert K to M
        if 'K' in val:
            return float(val.strip('K'))/1e3
        if 'k' in val:
            return float(val.strip('k'))/1e3
        # strip M
        if 'M' in val:
            return float(val.strip('M'))

    return float(val)


def yahoo_data(ticker):

    ## yf info dict keys ##
    # dict_keys(['zip', 'sector', 'fullTimeEmployees', 'longBusinessSummary', 'city', 'phone', 'state', 'country',
    #           'companyOfficers', 'website', 'maxAge', 'address1', 'industry', 'previousClose', 'regularMarketOpen',
    #           'twoHundredDayAverage', 'trailingAnnualDividendYield', 'payoutRatio', 'volume24Hr', 'regularMarketDayHigh',
    #           'navPrice', 'averageDailyVolume10Day', 'totalAssets', 'regularMarketPreviousClose', 'fiftyDayAverage',
    #           'trailingAnnualDividendRate', 'open', 'toCurrency', 'averageVolume10days', 'expireDate', 'yield', 'algorithm',
    #           'dividendRate', 'exDividendDate', 'beta', 'circulatingSupply', 'startDate', 'regularMarketDayLow', 'priceHint',
    #           'currency', 'regularMarketVolume', 'lastMarket', 'maxSupply', 'openInterest', 'marketCap',
    #           'volumeAllCurrencies', 'strikePrice', 'averageVolume', 'priceToSalesTrailing12Months', 'dayLow', 'ask',
    #           'ytdReturn', 'askSize', 'volume', 'fiftyTwoWeekHigh', 'forwardPE', 'fromCurrency', 'fiveYearAvgDividendYield',
    #           'fiftyTwoWeekLow', 'bid', 'tradeable', 'dividendYield', 'bidSize', 'dayHigh', 'exchange', 'shortName',
    #           'longName', 'exchangeTimezoneName', 'exchangeTimezoneShortName', 'isEsgPopulated', 'gmtOffSetMilliseconds',
    #           'quoteType', 'symbol', 'messageBoardId', 'market', 'annualHoldingsTurnover', 'enterpriseToRevenue',
    #           'beta3Year', 'profitMargins', 'enterpriseToEbitda', '52WeekChange', 'morningStarRiskRating', 'forwardEps',
    #           'revenueQuarterlyGrowth', 'sharesOutstanding', 'fundInceptionDate', 'annualReportExpenseRatio', 'bookValue',
    #           'sharesShort', 'sharesPercentSharesOut', 'fundFamily', 'lastFiscalYearEnd', 'heldPercentInstitutions',
    #           'netIncomeToCommon', 'trailingEps', 'lastDividendValue', 'SandP52WeekChange', 'priceToBook',
    #           'heldPercentInsiders', 'nextFiscalYearEnd', 'mostRecentQuarter', 'shortRatio', 'sharesShortPreviousMonthDate',
    #           'floatShares', 'enterpriseValue', 'threeYearAverageReturn', 'lastSplitDate', 'lastSplitFactor', 'legalType',
    #           'lastDividendDate', 'morningStarOverallRating', 'earningsQuarterlyGrowth', 'dateShortInterest', 'pegRatio',
    #           'lastCapGain', 'shortPercentOfFloat', 'sharesShortPriorMonth', 'category', 'fiveYearAverageReturn',
    #           'regularMarketPrice', 'logo_url'])
    ## ##

    try:
        share = yf.Ticker(ticker)
        info = share.info
        holders = share.institutional_holders['Holder'].values
        holds = ', '.join([x for x in holders])
        return info, holds
    except Exception as e:
        print('error checking ticker: ', ticker, e)
        return {}, ''


def yahoo_dict(table):

    rows = table.find_all('tr')
    data = []
    for row in rows[1:]:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append([ele for ele in cols if ele])

    dict = {}
    for d in data:
        dict[d[0]] = d[1:]

    return dict



def extra_yahoo_data(ticker):

    url = 'https://uk.finance.yahoo.com/quote/{}/key-statistics?p={}'.format(ticker, ticker)
    headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"}

    data = {}

    try:
        #table = pd.read_html(url) # Requires user agent headers
        req = requests.get(url, headers=headers)
        html = req.text
        soup = BeautifulSoup(html, features="lxml")
        table = soup.find('section', {"data-test": "qsp-statistics"})
        stats_dict = yahoo_dict(table)
        stats_df = pd.DataFrame.from_dict(stats_dict, orient='index')
    except Exception as e:
        data = {}
        data['ev_to_rev'], data['operating_margin'] = '', ''
        data['roa'], data['roe'] = '', ''
        data['rev'], data['rev_per_share'], data['rev_growth_yoy'] = '', '', ''
        data['ebitda'], data['eps_growth_yoy'] = '', ''
        data['total_cash'], data['cash_per_share'], data['debt_to_equity'] = '', '', ''
        data['current_ratio'], data['book_per_share'] = '', ''
        data['operating_cash_flow_ttm'], data['levered_fcf_ttm'] = '', ''
        print('error with extra yahoo data: ', ticker, e)
        return data

    # valuation measures
    #print(stats_df)
    data['ev_to_rev'] = check_nan(stats_df[0]['Enterprise value/revenue'])

    # stock price history
    # share statistics
    # dividends and splits
    # fiscal year

    # profitability
    data['operating_margin'] = check_nan(stats_df[0]['Operating margin (ttm)'])

    # management effectiveness
    data['roa'] = check_nan(stats_df[0]['Return on assets (ttm)'])
    data['roe'] = check_nan(stats_df[0]['Return on equity (ttm)'])

    # income statement
    data['rev'] = check_nan(stats_df[0]['Revenue (ttm)'])
    data['rev_per_share'] = check_nan(stats_df[0]['Revenue per share (ttm)'])
    data['rev_growth_yoy'] = check_nan(stats_df[0]['Quarterly revenue growth (yoy)'])
    data['ebitda'] = check_nan(stats_df[0]['EBITDA'])
    data['eps_growth_yoy'] = check_nan(stats_df[0]['Quarterly revenue growth (yoy)'])

    # balance sheet
    data['total_cash'] = check_nan(stats_df[0]['Total cash (mrq)'])
    data['cash_per_share'] = check_nan(stats_df[0]['Total cash per share (mrq)'])
    data['debt_to_equity'] = check_nan(stats_df[0]['Total debt/equity (mrq)'])
    data['current_ratio'] = check_nan(stats_df[0]['Current ratio (mrq)'])
    data['book_per_share'] = check_nan(stats_df[0]['Book value per share (mrq)'])

    # cash flow
    data['operating_cash_flow_ttm'] = check_nan(stats_df[0]['Operating cash flow (ttm)'])
    data['levered_fcf_ttm'] = check_nan(stats_df[0]['Levered free cash flow (ttm)'])

    return data


def yahoo_growth_analysis(ticker):

    url = 'https://uk.finance.yahoo.com/quote/{}/analysis?p={}'.format(ticker, ticker)
    headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"}

    data = {}

    # sales growth
    try:
        #table = pd.read_html(url)
        req = requests.get(url, headers=headers)
        html = req.text
        soup = BeautifulSoup(html, features="lxml")
        # sales growth
        table = soup.find('table', {"data-reactid": "86"})
        sales_dict = yahoo_dict(table)
        sales_df = pd.DataFrame.from_dict(sales_dict, orient='index', columns=['Current qtr', 'Next qtr',
                                                                               'Current year', 'Next year'])
        # eps history
        table = soup.find('table', {"data-reactid": "178"})
        eps_dict = yahoo_dict(table)
        # growth estimates
        table = soup.find('table', {"data-reactid": "387"})
        growth_dict = yahoo_dict(table)
        growth_df = pd.DataFrame.from_dict(growth_dict, orient='index',
                                           columns=[ticker, 'Industry', 'Sector', 'S&P 500'])
        #growth_df = pd.DataFrame.from_dict(growth_dict, orient='index',
        #                                   columns=[ticker])
    except Exception as e:
        data['curr_q_sales_growth'], data['next_q_sales_growth'] = '', ''
        data['curr_year_sales_growth'], data['next_year_sales_growth'] = '', ''
        data['earnings_curr_q'], data['earnings_next_q'] = '', ''
        data['earnings_curr_year'], data['earnings_next_year'] = '', ''
        data['eps_beats'], data['eps_accel'], data['eps_accel_y'] = '', '', ''
        data['sales_accel'], data['sales_accel_y'] = '', ''
        data['earnings_next_5y'], data['earnings_past_5y'] = '', ''
        print('error with yahoo growth data: ', ticker, e)
        return data

    # revenue estimate
    data['curr_q_sales_growth'] = check_nan(sales_df['Current qtr']['Sales growth (year/est)'])
    data['next_q_sales_growth'] = check_nan(sales_df['Next qtr']['Sales growth (year/est)'])
    data['curr_year_sales_growth'] = check_nan(sales_df['Current year']['Sales growth (year/est)'])
    data['next_year_sales_growth'] = check_nan(sales_df['Next year']['Sales growth (year/est)'])

    try:
        sales_accel = ((unit_convert(sales_df['Next qtr']['Year ago sales']) - unit_convert(sales_df['Current qtr']['Year ago sales'])) /
                        unit_convert(sales_df['Current qtr']['Year ago sales'])) * 100.
    except Exception as e:
        sales_accel = ''

    if sales_accel != '':
        sales_accel = check_nan(check_round(sales_accel))+'%'
    data['sales_accel'] = sales_accel

    try:
        sales_accel_y = ((unit_convert(sales_df['Next year']['Year ago sales']) - unit_convert(sales_df['Current year']['Year ago sales'])) /
                        unit_convert(sales_df['Current year']['Year ago sales'])) * 100.
    except Exception as e:
        sales_accel_y = ''

    if sales_accel_y != '':
        sales_accel_y = check_nan(check_round(sales_accel_y))+'%'
    data['sales_accel_y'] = sales_accel_y

    # earnings history
    eps_df = pd.DataFrame.from_dict(eps_dict, orient='index', columns=['q-4', 'q-3', 'q-2', 'q-1'])

    four, three, two, one = check_nan(eps_df['q-4']['Surprise %']), check_nan(eps_df['q-3']['Surprise %']), \
                            check_nan(eps_df['q-2']['Surprise %']), check_nan(eps_df['q-1']['Surprise %'])

    if one == '':
        one = '0'
    if two == '':
        two = '0'
    if three == '':
        three = '0'
    if four == '':
        four = '0'
    data['eps_beats'] = four + ', ' + three + ', ' + two + ', ' + one

    try:
        eps_accel = ((unit_convert(eps_df['q-1']['Actual EPS']) - unit_convert(eps_df['q-2']['Actual EPS'])) /
                     unit_convert(eps_df['q-2']['Actual EPS'])) * 100.
    except Exception as e:
        eps_accel = ''

    if eps_accel != '':
        eps_accel = check_nan(check_round(eps_accel))+'%'
    data['eps_accel'] = eps_accel

    try:
        eps_accel_y = ((unit_convert(eps_df['q-1']['Actual EPS']) - unit_convert(eps_df['q-4']['Actual EPS'])) /
                       unit_convert(eps_df['q-4']['Actual EPS'])) * 100.
    except Exception as e:
        eps_accel_y = ''

    if eps_accel_y != '':
        eps_accel_y = check_nan(check_round(eps_accel_y))+'%'
    data['eps_accel_y'] = eps_accel_y

    # growth estimates
    data['earnings_curr_q'] = check_nan(growth_df[ticker]['Current qtr.'])
    data['earnings_next_q'] = check_nan(growth_df[ticker]['Next qtr.'])
    data['earnings_curr_year'] = check_nan(growth_df[ticker]['Current year'])
    data['earnings_next_year'] = check_nan(growth_df[ticker]['Next year'])
    data['earnings_past_5y'] = check_nan(growth_df[ticker]['Past 5 years (per annum)'])
    data['earnings_next_5y'] = check_nan(growth_df[ticker]['Next 5 years (per annum)'])

    return data


def scrape_whalewisdom(ticker):

    # make url
    url = 'https://whalewisdom.com/stock/{}'.format(ticker)

    page = requests.get(url, stream=True)

    tree = html.fromstring(page.content)

    try:
        holders = tree.xpath('/html/body/div[2]/div[3]/div[3]/div[1]/div[3]/table/tbody/tr[2]/td[2]/text()')[0]
        priorq_holders = tree.xpath('/html/body/div[2]/div[3]/div[3]/div[1]/div[3]/table/tbody/tr[2]/td[3]/text()')[0].split()[0]
        new_positions = tree.xpath('/html/body/div[2]/div[3]/div[3]/div[1]/div[3]/table/tbody/tr[5]/td[2]/text()')[0].split()[0]
        new_positions_lastq = tree.xpath('/html/body/div[2]/div[3]/div[3]/div[1]/div[3]/table/tbody/tr[5]/td[3]/text()')[0].split()[0]
        shares = tree.xpath('/html/body/div[2]/div[3]/div[3]/div[1]/div[3]/table/tbody/tr[3]/td[2]/text()')[0]
        shares_lastq = tree.xpath('/html/body/div[2]/div[3]/div[3]/div[1]/div[3]/table/tbody/tr[3]/td[3]/text()')[0]
        shares_lastq = shares_lastq.split()[0]+' '+shares_lastq.split()[1]
        own = tree.xpath('/html/body/div[2]/div[3]/div[3]/div[1]/div[3]/table/tbody/tr[4]/td[2]/text()')[0].split()[0]
    except Exception as e:
        data = {'holders': '',
                'priorq_holders': '',
                'new_positions': '',
                'new_positions_lastq': '',
                'shares': '',
                'shares_lastq': '',
                'own': ''
                }
        print('error with whale wisdom: ', ticker, e)
        return data

    data = {'holders': holders,
            'priorq_holders': priorq_holders,
            'new_positions': new_positions,
            'new_positions_lastq': new_positions_lastq,
            'shares': shares,
            'shares_lastq': shares_lastq,
            'own': own
    }

    return data


def scrape_openinsider(ticker):

    # 1Y insiders
    url = 'http://www.openinsider.com/screener?s={}&o=&pl=&ph=&ll=&lh=&fd=365&fdr=&td=0&tdr=&fdlyl=&fdlyh=&' \
          'daysago=&xp=1&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&' \
          'oc2l=&oc2h=&sortcol=0&cnt=100&page=1'.format(ticker)

    try:
        table = pd.read_html(url)[11]['Value']
        num_buys = len(table.values)
        values = [int(x.strip('+$').replace(',', '')) for x in table.values]
        total_value = np.sum(values)

        data = {'1Y_buys': num_buys,
                '1Y_totalval': str(total_value)
        }
    except KeyError as e:
        data = {'1Y_buys': '',
                '1Y_totalval': ''
                }
        print('error with openinsider: ', ticker, e)

    # 6M insiders
    url = 'http://www.openinsider.com/screener?s={}&o=&pl=&ph=&ll=&lh=&fd=180&fdr=&td=0&tdr=&fdlyl=&fdlyh=&' \
          'daysago=&xp=1&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&' \
          'v2h=&oc2l=&oc2h=&sortcol=0&cnt=100&page=1'.format(ticker)

    try:
        table = pd.read_html(url)[11]['Value']
        num_buys = len(table.values)
        values = [int(x.strip('+$').replace(',', '')) for x in table.values]
        total_value = np.sum(values)

        data['6M_buys'] = num_buys
        data['6M_totalval'] = str(total_value)
    except KeyError as e:
        data['6M_buys'] = ''
        data['6M_totalval'] = ''

    return data


def scrape_earnings(ticker):

    url = 'https://stocksearning.com/stocks/{}/earnings-date'.format(ticker)

    try:
        page = requests.get(url, stream=True)
        tree = html.fromstring(page.content)

        earndate = tree.xpath('//*[@id="ContentPlaceHolder1_lblEarningDate"]/text()')[0]
    except Exception as e:
        earndate = ''
        print('error with earnings date: ', ticker, e)
    return earndate


def get_cik(ciks, ticker):

    return ciks.loc[(ticker).lower()][0]


def scrape_finviz(ticker):
    #{'Company': 'Futu Holdings Limited', 'Sector': 'Financial', 'Industry': 'Capital Markets', 'Country': 'Hong Kong',
    # 'Index': '-', 'P/E': '233.62', 'EPS (ttm)': '0.81', 'Insider Own': '0.99%', 'Shs Outstand': '130.19M',
    # 'Perf Week': '20.57%', 'Market Cap': '23.25B', 'Forward P/E': '87.62', 'EPS next Y': '85.68%',
    # 'Insider Trans': '0.00%', 'Shs Float': '52.11M', 'Perf Month': '110.87%', 'Income': '105.10M', 'PEG': '13.54',
    # 'EPS next Q': '0.45', 'Inst Own': '50.70%', 'Short Float': '31.91%', 'Perf Quarter': '316.92%', 'Sales': '314.10M',
    # 'P/S': '74.03', 'EPS this Y': '266.90%', 'Inst Trans': '25.11%', 'Short Ratio': '2.26', 'Perf Half Y': '488.76%',
    # 'Book/sh': '5.65', 'P/B': '33.58', 'ROA': '1.90%', 'Target Price': '86.95', 'Perf Year': '1387.84%',
    # 'Cash/sh': '36.02', 'P/C': '5.27', 'EPS next 5Y': '17.25%', 'ROE': '23.40%', '52W Range': '8.16 - 204.25',
    # 'Perf YTD': '314.64%', 'Dividend': '-', 'P/FCF': '-', 'EPS past 5Y': '-', 'ROI': '4.40%', '52W High': '-11.95%',
    # 'Beta': '-', 'Dividend %': '-', 'Quick Ratio': '-', 'Sales past 5Y': '-', 'Gross Margin': '77.80%',
    # '52W Low': '2103.92%', 'ATR': '20.23', 'Employees': '847', 'Current Ratio': '-', 'Sales Q/Q': '26.10%',
    # 'Oper. Margin': '38.20%', 'RSI (14)': '68.97', 'Volatility': '12.77% 14.71%', 'Optionable': 'Yes',
    # 'Debt/Eq': '5.35', 'EPS Q/Q': '240.20%', 'Profit Margin': '33.40%', 'Rel Volume': '1.07', 'Prev Close': '189.70',
    # 'Shortable': 'Yes', 'LT Debt/Eq': '0.00', 'Earnings': 'Nov 19 BMO', 'Payout': '0.00%', 'Avg Volume': '7.36M',
    # 'Price': '179.84', 'Recom': '1.70', 'SMA20': '35.21%', 'SMA50': '110.59%', 'SMA200': '314.93%',
    # 'Volume': '5,792,861', 'Change': '-5.20%'}

    try:
        stock = finvizfinance(ticker)
    except Exception as e:
        return {}

    # get fundamental data from finviz table
    return stock.ticker_fundament()


def get_up_down_vol(ticker):

    price_data = yf.download(ticker, start=dt.datetime.today()-dt.timedelta(days=90))

    close = price_data['Adj Close']
    open = price_data['Open']
    price_diff = (close - open).iloc[-65:]
    vol = price_data['Volume'].iloc[-65:]
    avg_vol = vol.rolling(window=30).mean()

    try:
        upvol_count = np.count_nonzero(vol[(price_diff > 0) & (vol >= avg_vol)])
        downvol_count = np.count_nonzero(vol[(price_diff < 0) & (vol >= avg_vol)])
        updown_vol = upvol_count / downvol_count
    except ZeroDivisionError:
        upvol_count = np.count_nonzero(vol[(price_diff > 0)])
        downvol_count = np.count_nonzero(vol[(price_diff < 0)])
        updown_vol = upvol_count / downvol_count

    return updown_vol


def main():

    parser = argparse.ArgumentParser(description="Watchlist Updater")
    parser.add_argument('--all', help="Update all data", action='store_true')
    parser.add_argument('--new', help="Update new tickers only", action='store_true')
    parser.add_argument('--basic', help="Update basic data only", action='store_true')
    parser.add_argument('--eps', help="Update EPS data only", action='store_true')
    parser.add_argument('--rev', help="Update revenue data only", action='store_true')
    parser.add_argument('--fundamental', help="Update fundamental & ratio data only", action='store_true')
    parser.add_argument('--insider', help="Update insider buying", action='store_true')
    parser.add_argument('--whale', help="Update whale data", action='store_true')
    parser.add_argument('--fromticker', help="Continue from ticker", type=str)

    args = parser.parse_args()

    new_only = args.new#False
    alldata = args.all#True
    basiconly = args.basic#False
    epsonly = args.eps#False
    revonly = args.rev#False
    fundamentalonly = args.fundamental#False
    insidersonly = args.insider#False
    whalesonly = args.whale#False
    fromticker = args.fromticker

    if len(sys.argv) <= 1:
        print("* NO ARGUMENT SPECIFIED *")
        parser.print_help()
        sys.exit(0)

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    print('** WATCHLIST UPDATER **')

    if alldata:
        print('Updating all data')
    if new_only:
        print('Updating new tickers only')
        alldata = True
    if basiconly:
        print('Only updating basic data')
    if epsonly:
        print('Only updating EPS data')
    if revonly:
        print('Only updating REV data')
    if fundamentalonly:
        print('Only updating fundamental data and ratios')
    if insidersonly:
        print('Only updating insiders data')
    if whalesonly:
        print('Only updating whales data')
    if fromticker is not None:
        skip = True
        print('Continuing from ticker: ' + str(fromticker))

    # load in prior tickers to check for new
    try:
        prior_tickers = pd.read_csv('prior_tickers.csv', header=None, index_col=0)
        print('Loaded csv of tickers')
        print('Prior tickers: \n', prior_tickers)
    except Exception as e:
        prior_tickers = []
        print('Did not load csv of tickers')

    try:
        ciks = pd.read_csv('ticker_cik.csv', index_col=0)
        print('Loaded CIKS')
    except:
        print('Error loading CIKS')

    # get tickers from ghseets
    print('Getting tickers from gsheet')
    maintable, tickers = get_watchlist_tickers(config['gsheet_url'])
    print('Got tickers from gsheet')

    # check for new tickers
    new_tickers = []
    done_tickers = []
    for ticker in tickers:
        if ticker == "":
            continue
        if ticker not in prior_tickers[1].values:
            new_tickers.append(ticker)
        else:
            done_tickers.append(ticker)
    if len(new_tickers) > 0:
        print('New tickers:', new_tickers)

    # FOR TESTING
    #tickers = ['SGBX', 'OEG', 'FTEK', 'HIMX', 'DDD']

    basicdata = ['E', 'F', 'G', 'H', 'I', 'J', 'M', 'V']
    epsdata = ['X', 'Y', 'Z', 'AA', 'AB', 'AD', 'AE', 'AF', 'AG', 'AI']
    revdata = ['AJ', 'AK', 'AL', 'AM', 'AO', 'AP', 'AQ', 'AT', 'AR']
    fundamentaldata = ['AU', 'AV', 'AW', 'AY', 'AZ', 'BA',
                       'BB', 'BC', 'BF', 'BG', 'BI', 'BJ',
                       'BL', 'BM', 'BN', 'BO', 'BP', 'BQ',
                       'BR', 'BS', 'BT', 'BU']
    insiderdata = ['BX', 'BY', 'BZ', 'CA', 'CB']
    whaledata = ['CC', 'CD', 'CE', 'CF', 'CG' ,'CH', 'CI', 'CJ']

    alldata_cols = basicdata + epsdata + revdata + fundamentaldata + insiderdata + whaledata

    print('Only update new tickers:', new_only)
    # loop through tickers are start scraping data for each one and updating gsheet
    for i, ticker in enumerate(tickers):
        #if i < 4:
        #    continue
        # skip empty cell but need to keep for row number
        if ticker == "":
            continue
        if new_only:
            if ticker not in new_tickers:
                print('Skipping: ', ticker)
                #done_tickers.append(ticker)
                continue
            row = maintable.find(ticker).row
        else:
            # offset index for row in gsheet
            row = i+3

        if fromticker is not None:
            if ticker == fromticker:
                skip = False
            if skip:
                print('Skipping: ', ticker)
                row = i + 3
                continue

        print('Getting data for:', ticker, ' at ', 'A'+str(row))

        if (alldata) or (basiconly):
            try:
                cik = str(get_cik(ciks, ticker))
            except:
                cik = ''

        if (alldata) or (basiconly) or (fundamentalonly):
            # get yfinance yahoo data
            yahoo_info, topholders = yahoo_data(ticker)

            if len(yahoo_info) == 0:
                print('Could not get yahoo data for: ', ticker)
                name, biz, sector, industry, beta, price_to_sales_ttm, forward_pe = '', '', '', '', '' , '', ''
                profit_margins, ev_to_ebitda, forward_eps, rev_qoq, eps_qoq = '', '', '', '', ''
                book_value, price_to_book, insider_own, float, ev, peg = '', '', '', '', '', ''
                maintable.update('C'+str(row), 'ERROR', value_input_option='USER_ENTERED')
            else:
                # yahoo data
                print('Getting yahoo data for: ', ticker)
                name = yahoo_info['shortName']
                biz = yahoo_info['longBusinessSummary']
                sector = yahoo_info['sector']
                industry = yahoo_info['industry']
                beta = check_round(yahoo_info['beta'])
                price_to_sales_ttm = check_round(yahoo_info['priceToSalesTrailing12Months'])
                forward_pe = check_round(yahoo_info['forwardPE'])
                profit_margins = check_round(yahoo_info['profitMargins'])
                ev_to_ebitda = check_round(yahoo_info['enterpriseToEbitda'])
                forward_eps = check_round(yahoo_info['forwardEps'])
                # better ones done below #
                #rev_qoq = check_round(yahoo_info['revenueQuarterlyGrowth'])
                #eps_qoq = check_round(yahoo_info['earningsQuarterlyGrowth'])
                # #
                book_value = check_round(yahoo_info['bookValue'])
                price_to_book = check_round(yahoo_info['priceToBook'])
                insider_own = check_round(yahoo_info['heldPercentInsiders'])
                inst_own = check_round(yahoo_info['heldPercentInstitutions'])
                try:
                    float = str(check_round(yahoo_info['floatShares']/1e6))#+'M'
                except TypeError:
                    float = ''
                try:
                    ev = str(check_round(yahoo_info['enterpriseValue']/1e6))+'M'
                except TypeError:
                    ev = ''
                peg = check_round(yahoo_info['pegRatio'])

        if (alldata) or (fundamentalonly):
            # scrape extra data fro yahoo statistics page
            extra_yahoo_info = extra_yahoo_data(ticker)

            print('Getting extra yahoo data for: ', ticker)
            ev_to_rev = extra_yahoo_info['ev_to_rev']
            operating_margin = extra_yahoo_info['operating_margin']
            roa = extra_yahoo_info['roa']
            roe = extra_yahoo_info['roe']
            rev = extra_yahoo_info['rev']
            rev_per_share = extra_yahoo_info['rev_per_share']
            # better ones done below #
            rev_growth_yoy = extra_yahoo_info['rev_growth_yoy']
            eps_growth_yoy = extra_yahoo_info['eps_growth_yoy']
            # #
            ebitda = extra_yahoo_info['ebitda']
            total_cash = extra_yahoo_info['total_cash']
            cash_per_share = extra_yahoo_info['cash_per_share']
            debt_to_equity = extra_yahoo_info['debt_to_equity']
            current_ratio = extra_yahoo_info['current_ratio']
            book_per_share = extra_yahoo_info['book_per_share']
            operating_cash_flow_ttm = extra_yahoo_info['operating_cash_flow_ttm']
            levered_fcf_ttm = extra_yahoo_info['levered_fcf_ttm']

        if (alldata) or (epsonly) or (revonly):
            print('Getting yahoo growth data for: ', ticker)
            # get yahoo earnings and sales growth analysis
            growth_analysis = yahoo_growth_analysis(ticker)
            #print(growth_analysis)
            earnings_curr_q = growth_analysis['earnings_curr_q']
            earnings_next_q = growth_analysis['earnings_next_q']
            earnings_curr_year = growth_analysis['earnings_curr_year']
            earnings_next_year = growth_analysis['earnings_next_year']
            earnings_next_5y = growth_analysis['earnings_next_5y']
            earning_last_5y = growth_analysis['earnings_past_5y']
            curr_q_sales_growth = growth_analysis['curr_q_sales_growth']
            next_q_sales_growth = growth_analysis['next_q_sales_growth']
            curr_year_sales_growth = growth_analysis['curr_year_sales_growth']
            next_year_sales_growth = growth_analysis['next_year_sales_growth']
            eps_beats = growth_analysis['eps_beats']
            eps_accel = growth_analysis['eps_accel']
            eps_accel_y = growth_analysis['eps_accel_y']
            sales_accel = growth_analysis['sales_accel']
            sales_accel_y = growth_analysis['sales_accel_y']

        if (alldata) or (epsonly) or (revonly):
            # get finviz data table
            print('Getting finviz data for:', ticker)
            finvizdata = scrape_finviz(ticker)
            if len(finvizdata) == 0:
                eps_qoq, rev_qoq, gross_margin, eps_last_five_years, eps_next_five_years = '', '', '' , '' , ''
                earnings_curr_year, earnings_next_year, roa, roe, sales_last_five_years = '', '', '', '', ''
            else:
                #print(finvizdata)
                # only need some finviz data and to fill in for empty yahoo data
                # keys: Gross Margin, Sales Q/Q, EPS Q/Q, EPS this Y, EPS next Y
                eps_qoq = check_nan(finvizdata['EPS Q/Q'])
                rev_qoq = check_nan(finvizdata['Sales Q/Q'])
                gross_margin = check_nan(finvizdata['Gross Margin'])
                eps_last_five_years = check_nan(finvizdata['EPS past 5Y'])
                eps_next_five_years = check_nan(finvizdata['EPS next 5Y'])
                if eps_last_five_years == '':
                    eps_last_five_years = earning_last_5y
                if eps_next_five_years == '':
                    eps_next_five_years = earnings_next_5y
                sales_last_five_years = check_nan(finvizdata['Sales past 5Y'])
                if earnings_curr_year == '':
                    earnings_curr_year = check_nan(finvizdata['EPS this Y'])
                if earnings_next_year == '':
                    earnings_next_year = check_nan(finvizdata['EPS next Y'])
                if roa == '':
                    roa = check_nan(finvizdata['ROA'])
                if roe == '':
                    roe = check_nan(finvizdata['ROE'])

        if (alldata) or (whalesonly):
            # get institutional data from whalewidsom
            whales = scrape_whalewisdom(ticker)
            nd = len(whales)
            emp = 0
            for k, v in whales.items():
                if v == '':
                    emp += 1
            if emp == nd:
                whales = scrape_whalewisdom(ticker+'-2')
            print('Getting whales data for: ', ticker)
            whale_holders = whales['holders']
            whale_priorq_holders = whales['priorq_holders']
            whale_new_positions = whales['new_positions']
            whale_new_positions_lastq = whales['new_positions_lastq']
            whale_shares = whales['shares']
            whale_shares_lastq = whales['shares_lastq']
            whales_own = str(check_round(whales['own']))+'%'
            if whales_own == '%':
                whales_own = ''

            if (whale_holders == '') or (whale_priorq_holders == ''):
                whale_holders_q_change = ''
            else:
                whale_holders_q_change = str(int(whale_holders) - int(whale_priorq_holders))
            if (whale_new_positions == '') or (whale_new_positions_lastq == ''):
                whale_new_positions_q_change = ''
            else:
                whale_new_positions_q_change = str(int(whale_new_positions) - int(whale_new_positions_lastq))

            whale_shares_q_change = ''#whale_shares - whale_shares_lastq

        if (alldata) or (insidersonly):
            # get insider buys from openinsider
            #data = {'1Y_buys': num_buys,
            #        '1Y_totalval': total_value,
            #        '6M_buys': num_buys,
            #        '6M_totalval': total_value
            #        }
            insiders = scrape_openinsider(ticker)
            print('Getting insider data for: ', ticker)
            insider_year_buys = insiders['1Y_buys']
            insider_year_totalval = insiders['1Y_totalval']
            insider_6mon_buys = insiders['6M_buys']
            insider_6mon_totalval = insiders['6M_totalval']

        if (alldata) or (basiconly):
            print('Getting U/D Vol for:', ticker)
            updown_vol = check_nan(check_round(get_up_down_vol(ticker)))

            print('Getting earnings date for: ', ticker)
            earndate = scrape_earnings(ticker)

        print('Updating values in sheets for: ', ticker)

        # update this info to gsheets
        basicset, epsset, revset, fundamentalset, insiderset, whaleset = [], [], [], [], [], []
        if (alldata) or (basiconly):
            basicset = [(basicdata[0] + str(row), name),
            (basicdata[1] + str(row), biz),
            (basicdata[2] + str(row), cik),
            (basicdata[3] + str(row), sector),
            (basicdata[4] + str(row), industry),
            (basicdata[5] + str(row), earndate),
            (basicdata[6] + str(row), float),
            (basicdata[7] + str(row), updown_vol)]
        if (alldata) or (epsonly):
            epsset = [(epsdata[0] + str(row), eps_qoq),
            (epsdata[1] + str(row), eps_beats),
            (epsdata[2] + str(row), earnings_curr_q),
            (epsdata[3] + str(row), earnings_next_q),
            (epsdata[4] + str(row), eps_accel),
            (epsdata[5] + str(row), eps_last_five_years),
            (epsdata[6] + str(row), earnings_curr_year),
            (epsdata[7] + str(row), earnings_next_year),
            (epsdata[8] + str(row), eps_accel_y),
            (epsdata[9] + str(row), eps_next_five_years)]
        if (alldata) or (revonly):
            revset = [(revdata[0] + str(row), rev_qoq),
            (revdata[1] + str(row), curr_q_sales_growth),
            (revdata[2] + str(row), next_q_sales_growth),
            (revdata[3] + str(row), sales_accel),
            (revdata[4] + str(row), sales_last_five_years),
            (revdata[5] + str(row), curr_year_sales_growth),
            (revdata[6] + str(row), next_year_sales_growth),
            (revdata[7] + str(row), gross_margin),
            (revdata[8] + str(row), sales_accel_y)]
        if (alldata) or (fundamentalonly):
            fundamentalset = [(fundamentaldata[0] + str(row), profit_margins),
            (fundamentaldata[1] + str(row), operating_margin),
            (fundamentaldata[2] + str(row), rev),
            (fundamentaldata[3] + str(row), ev_to_rev),
            (fundamentaldata[4] + str(row), rev_per_share),
            (fundamentaldata[5] + str(row), price_to_sales_ttm),
            (fundamentaldata[6] + str(row), price_to_book),
            (fundamentaldata[7] + str(row), peg),
            (fundamentaldata[8] + str(row), forward_pe),
            (fundamentaldata[9] + str(row), ev),
            (fundamentaldata[10] + str(row), ev_to_ebitda),
            (fundamentaldata[11] + str(row), ebitda),
            (fundamentaldata[12] + str(row), beta),
            (fundamentaldata[13] + str(row), book_per_share),
            (fundamentaldata[14] + str(row), roa),
            (fundamentaldata[15] + str(row), roe),
            (fundamentaldata[16] + str(row), total_cash),
            (fundamentaldata[17] + str(row), cash_per_share),
            (fundamentaldata[18] + str(row), debt_to_equity),
            (fundamentaldata[19] + str(row), current_ratio),
            (fundamentaldata[20] + str(row), operating_cash_flow_ttm),
            (fundamentaldata[21] + str(row), levered_fcf_ttm)]
        if (alldata) or (insidersonly):
            insiderset = [(insiderdata[0] + str(row), insider_year_buys),
            (insiderdata[1] + str(row), insider_year_totalval),
            (insiderdata[2] + str(row), insider_6mon_buys),
            (insiderdata[3] + str(row), insider_6mon_totalval),
            (insiderdata[4] + str(row), insider_own)]
        if (alldata) or (whalesonly):
            whaleset = [(whaledata[0] + str(row), whale_holders),
            (whaledata[1] + str(row), whale_holders_q_change),
            (whaledata[2] + str(row), whale_new_positions),
            (whaledata[3] + str(row), whale_new_positions_q_change),
            (whaledata[4] + str(row), whale_shares),
            (whaledata[5] + str(row), whale_shares_q_change),
            (whaledata[6] + str(row), whales_own),
            (whaledata[7] + str(row), topholders)]

        api_calls = basicset + epsset + revset + fundamentalset + insiderset + whaleset

        print('Processing API calls for:', ticker)
        for call in api_calls:
            #print(call[0], call[1], type(call[1]))
            # don't waste time with empty data
            #if (call[1] == ''):
            #    continue
            try:
                maintable.update(call[0], call[1], value_input_option='USER_ENTERED')
            except TypeError:
                maintable.update(call[0], str(call[1]), value_input_option='USER_ENTERED')
            except Exception as e:
                print('Data push error: ', e)
                print('Skipping call for: ', call[0], call[1])
                sleep(1.2)
                continue
            sleep(1.2)

        print('Done scraping: ', ticker)

        if new_only:
            done_tickers.append(ticker)
        else:
            if ticker in new_tickers:
                done_tickers.append(ticker)

        # save tickers for keeping track what has been updated
        try:
            df = pd.DataFrame(done_tickers)
            df.to_csv('prior_tickers.csv', header=False)
            print('Updated csv of tickers')
        except Exception as e:
            print('Failed to update csv of tickers')
            pass

        # sleep a second just to delay a little
        sleep(5)


    # remove empty strings for cells i've deleted a ticker
    while("" in tickers):
        tickers.remove("")

    print('** COMPLETE **')

if __name__ == '__main__': main()