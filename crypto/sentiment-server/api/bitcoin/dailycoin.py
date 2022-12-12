from bs4 import BeautifulSoup
import urllib.request, sys, time
import pandas as pd
import requests
from db import database as db_manager
import time
import datetime
import tunnel
# this wiill be a bitcoin specific sentiment

urls = ['https://cryptodaily.co.uk/tag/bitcoinrobot/', 'https://newsbtc.com/news/bitcoin/']

def getpagecontents(pageurl):
    page = requests.get(pageurl)
    soup = BeautifulSoup(page.text, 'html.parser')
    paragraphs = soup.find_all('p')
    pagetext = ''
    for paragraph in paragraphs:
        spans = paragraph.find_all('span')
        for span in spans:
            pagetext += span.text.strip() + ' '
    time.sleep(1)
    return pagetext

def historical_dailycoin(db : db_manager) :
    for pagenum in range(1000):
        url = 'https://dailycoin.com/bitcoin/?page={pagenum}'
        page = requests.get(url)
        if page.status_code == 200:
            soup = BeautifulSoup(page.text, 'html.parser')
            article_cards = soup.find_all('div', attrs={'class' : 'mkd-post-item-inner'})
            for article in article_cards:
                titlelink = article.find('a', attrs={'class' : 'mkd-pt-title-link'})
                title = titlelink.text.strip().replace('\n', '')
                link = titlelink['href'].strip()
                author = article.find('span', attrs={'class' : 'post-info-author'}).text.rstrip().replace('by ', '').replace('\n', '').lstrip(' ')
                date = article.find('span', attrs={'class' : 'date'}).text.rstrip()
                entry_time = article.find('span', attrs={'class' : 'time'}).text.rstrip()
                articlecontents = getpagecontents(link)
                articledata = pd.DataFrame(data={'title': title, 'author' : author, 'date' : date, 'time' : entry_time, 'contents' : articlecontents, 'link' : link}, index=[0])

                # db.df_to_table(articledata, 'articles', 'title')
                try:
                    db.insert_df(articledata, 'articles')
                    print(title, '- Inserted into table')
                except Exception as e:
                    print('error', e)
                    print(title, 'Failed to insert')
        else:
            print(f'[{datetime.datetiem.now()}] All historical data collected for Daily Coin waiting for new data.')
            break
        print(f'Page: {pagenum}')

def automated_collection():
    url = 'https://dailycoin.com/bitcoin/'
    while True:
        pass

def main():

    db = db_manager()
    # with open('clean_conn.sql') as file:
    #     print(file.read())
    # db.run_file('clean_conn.sql')

    df = db.select_df('select * from articles')
    print(df)

    # historical_dailycoin(db)
    
    # print(db.get_all_table_info())
    # db.query('select * from schema')

    # time.sleep(5)




        



main()