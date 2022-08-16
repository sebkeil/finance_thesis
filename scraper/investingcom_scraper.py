"""
This webscraper downloads financial news PER STOCK and stores the information in a dataframe

- Methodology to avoid excessive scarcity:

- If full Reuters article available: scrape it
- If website not scrapabale (e.g. CNBC): scrape only headline
- Other articles: scrape sentences that mention the stock ticker


"""

import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import datetime

headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'}     # to avoid 404 request

# create dictionary to contain results, initiate iteratables keys
news_dict = {}
news_dict['url'] = []
news_dict['headline'] = []
news_dict['raw_article'] = []
news_dict['date'] = []
news_dict['ticker'] = []

# problem: barely any interesting news about companies like 3M

# dictionary that maps tickers to company names (as given in the investing.com URLs)
ticker_names = {"AAPL": "apple-computer-inc",
                "AMGN": "amgen-inc",
                "AXP": "american-express",
                "BA": "boeing-co",
                "CAT": "caterpillar",
                "CSCO": "cisco-sys-inc",
                "CVX": "chevron",
                "GS": "goldman-sachs-group",
                "HD": "home-depot",
                "HON": "honeywell-intl",
                "IBM": "ibm",
                "INTC": "intel-corp",
                "KO": "coca-cola-co",
                "JPM": "jp-morgan-chase",
                "MCD": "mcdonalds",
                "MMM": "3m-co",
                "MRK": "merck---co",
                "MSFT": "microsoft-corp",
                "NKE": "nike",
                "PG": "procter-gamble",
                "TRV": "the-travelers-co",
                "UNH": "united-health-group",
                "CRM": "salesforce-com",
                "VZ": "verizon-communications",
                "V": "visa-inc",
                "WBA": "walgreen-co",
                "WMT": "wal-mart-stores",
                "DIS": "disney",
                }

NUM_PAGES = 100   #100

def check_lenghts(news_dict):
    # check if everything has the same length
    if not len(news_dict['url']) == len(news_dict['headline']) == len(news_dict['raw_article']):
        # get the maximal length one
        max_len = max(
            [len(news_dict['url']), len(news_dict['headline']), len(news_dict['raw_article']), len(news_dict['date']),
             len(news_dict['ticker'])])
        # extend all lists with NaN to make them the same length
        news_dict['url'].extend(['NaN'] * (max_len - len(news_dict['url'])))
        news_dict['headline'].extend(['NaN'] * (max_len - len(news_dict['headline'])))
        news_dict['raw_article'].extend(['NaN'] * (max_len - len(news_dict['raw_article'])))
        news_dict['date'].extend(['NaN'] * (max_len - len(news_dict['date'])))
        news_dict['ticker'].extend(['NaN'] * (max_len - len(news_dict['ticker'])))


def clean_dates(date_str):
    # date_str = str(date_str)
    # filter out the brackets
    if "hours ago" or "minutes ago" in date_str:
        # date_str = str(re.findall(r'\(.*?\)', date_str))[3:-3]
        left_idx = date_str.find('(')
        right_idx = date_str.find(')', left_idx)

        date_str = date_str[left_idx + 1:right_idx]
        date_str = date_str.replace("ET", "")
        date_str = date_str.replace("E", "")

    return date_str

# function to check for most recent year
def check_recent_year(date_str):
    possible_years = ['2015','2016', '2017', '2018', '2019', '2020', '2021', '2022']

    current_year = None

    for p in possible_years:
        if p in date_str:
            current_year = int(p)

    return current_year

# write column names into file
f = open('fin_news_coll_01.csv', 'w+')
f.write('url,headline,raw_article,date,ticker\n')
f.close()


for ticker in ticker_names.keys():
    print(f"Scraping News for company: {ticker}")
    #for i in range(1, NUM_PAGES+1):

    latest_year = 2022

    i = 0

    # toDO: implement date logic for looping back to 2016
    # we want to collect data at least for the years 2017, 2018, 2019, 2020, 2021

    while latest_year > 2016:

        i += 1
        print(f'current page {i}')

        try:
            print(f"Page {i}")
            news_address = f"https://www.investing.com/equities/{ticker_names[ticker]}-news/{i}"
            html_page = requests.get(news_address, headers=headers).text
            soup = BeautifulSoup(html_page, 'lxml')
            articles = soup.find_all('article')
            print("FOUND ", len(articles), " ARTICLES")

            for j, article in enumerate(articles):
                article_title = article.find("a", class_ ='title')
                try:
                    url = "https://www.investing.com" + article_title.get('href')

                    if 'news/stock-market-news' in url and url not in news_dict['url']:                 # to make sure we don't pick up commodity, ads, analysis, etc... | avoid dublicates

                        headline = article_title.text
                        news_dict['url'].append(url)
                        news_dict['headline'].append(headline)
                        news_dict['ticker'].append(ticker)

                        # now: go to url and parse it
                        article_html = requests.get(url, headers=headers).text
                        article_soup = BeautifulSoup(article_html, 'lxml')

                        # locate and store the date
                        content_sect = article_soup.find('div', class_='contentSectionDetails')
                        date = content_sect.find('span').text
                        print("DATE:" , date)
                        news_dict['date'].append(date)

                        # update the date counting mechanism
                        clean_date = clean_dates(date)
                        current_year = check_recent_year(clean_date)
                        print('current year: ', current_year)

                        if current_year:
                            latest_year = current_year

                        article_div = article_soup.find('div', class_='WYSIWYG articlePage')
                        paragraphs = article_div.find_all('p')

                        raw_article = "".join([paragraph.text for paragraph in paragraphs])

                        # check if article is from the International Business Times (different parsing structure)
                        linkedd = paragraphs[0].find('a')
                        if linkedd:
                            if 'ibtimes' in linkedd.get('href'):
                                raw_article = "(IBT) " + raw_article

                        news_dict['raw_article'].append(raw_article)

                        # check if everything has the same length, if not: pad it
                        check_lenghts(news_dict)

                        # write line to file
                        f = open('fin_news_coll_01.csv', 'w+')
                        f.write(f'{url},{headline},{raw_article},{date},{ticker}\n')
                        f.close()

                except:
                    continue

        except:
            continue


# another check for same lenght
check_lenghts(news_dict)

# save as json for backup
with open('../files/checkpoints/rawnews.json', 'w') as outfile:
    json.dump(news_dict, outfile)

# convert to pandas and save as csv
news_df = pd.DataFrame(news_dict)
news_df.to_csv('../files/checkpoints/rawnews.csv')
