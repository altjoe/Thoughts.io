import traceback
from sentiment_server.db.db import Database
import pandas as pd
from bs4 import BeautifulSoup
import requests
import time 
import random
import datetime
# from simpletext import send_message

def getpagecontents(pageurl):
    page = requests.get(pageurl)
    soup = BeautifulSoup(page.text, 'html.parser')
    paragraphs = soup.find_all('p')
    authorlink = soup.find('div', attrs={'class' : 'authour_link'})
    author = authorlink.find('a').text.strip()
    datecountText = soup. find('div', attrs={'class' : 'date-count'}).text.strip()
    date = ' '.join(datecountText.split(' ')[-3:])
    pagetext = ''
    for paragraph in paragraphs:
        spans = paragraph.find_all('span')
        for span in spans:
            pagetext += span.text.strip() + ' '
    time.sleep(2)
    return pagetext, author, date

def cryptodaily_history(db, articlename):
    # send_message('cryptodaily Historical Starting...')
    pagenum = 1
    while True:
        try:
            titlestore = list(db.select_df(f'select * from {articlename}')['title'].values)
            # print(titlestore)
            break
        except:
            traceback.print_exc()
            print('No table yet')
            titlestore = []
            time.sleep(5)

    while True:
        url = f'https://cryptodaily.co.uk/tag/bitcoin/?page={pagenum}'
        print('Cryptodaily: ', url)
        page = requests.get(url)
        # print(page.status_code)
        if page.status_code == 200:
            soup = BeautifulSoup(page.text, 'html.parser')
            article_cards = soup.find_all('a', attrs={'class' : 'post-item'})
    
            for article in article_cards:
                title = article.find('h3', attrs={'class' : 'hb-title'}).text.strip().replace('\n', '')
                # print(title)
                link = article['href'].strip()
                pageContents, author, date = getpagecontents(link)
                articledata = pd.DataFrame(data={'title': title, 'author' : author, 'date' : date, 'contents' : pageContents, 'link' : link}, index=[0])
                if title not in titlestore:
                    # send_message(f'New news available Cryptodaily: {title}')
                    try:
                        db.insert_df(articledata, articlename)
                        titlestore.append(title)
                        print(f'{title} -> Inserted Properly Cryptodaily')
                    except Exception as e:
                        print(f' {e}\n[ERROR] Insert failed Cryptodaily: {title}')
                else:
                    probablyReachedEnd = True
                    print(f'No more old stories: {datetime.datetime.now()}')

        else:
            # send_message('cryptodaily Historical Closed. PageNum: {pagenum}')
            print(f'[ERROR] Page error: {page.status_code} Page Num: {pagenum}')
            break
        pagenum += 1
        time.sleep(2 + random.randint(1, 7))

def cryptodaily_stream(db, articlename):
    # send_message('cryptodaily Stream Starting...')

    while True:
        try:
            titlestore = list(db.select_df(f'select * from {articlename}')['title'].values)
            print(titlestore)
            break
        except:
            print('No table yet')
            titlestore = []
            time.sleep(5)

    while True:
        url = f'https://cryptodaily.co.uk/tag/bitcoin/'
        
        page = requests.get(url)

        if page.status_code == 200:
            soup = BeautifulSoup(page.text, 'html.parser')
            article_cards = soup.find_all('div', attrs={'class' : 'post-item'})
            
            for article in article_cards:
                titlelink = article.find('h3', attrs={'class' : 'list-title'}).find('a')
                title = titlelink.text.strip().replace('\n', '')
                link = titlelink['href'].strip()
                author = article.find('a', attrs={'class' : 'breakin-item-author'}).text.rstrip().lstrip().replace('\n', '').replace('by', '')
                date = article.find('a', attrs={'class' : 'breakin-item-time'}).text.rstrip().lstrip()
                articlecontents = getpagecontents(link)
                articledata = pd.DataFrame(data={'title': title, 'author' : author, 'date' : date, 'contents' : articlecontents, 'link' : link}, index=[0])
                
                if title not in titlestore:
                    # send_message(f'New news available Cryptodaily: {title}')
                    print(f'New news available Cryptodaily: {title}')
                    try:
                        db.insert_df(articledata, articlename)
                        titlestore.append(title)
                        print(f'{title} -> Inserted Properly Cryptodaily')
                    except Exception as e:
                        print(f' {e}\n[ERROR] Insert failed Cryptodaily: {title}')
                else:
                    
                    print(f'No more new stories: {datetime.datetime.now()}')
                    break
                    
            
        else:
            print(f'[ERROR] Page error: {page.status_code}')

        print('Tried...')
        time.sleep(15 * 60 + random.randint(-30, 30))

def main():
    with Database() as db:
        cryptodaily_history(db, 'btc_article_cryptodaily')

def stream():
    with Database() as db:
        cryptodaily_stream(db, 'btc_article_cryptodaily')


