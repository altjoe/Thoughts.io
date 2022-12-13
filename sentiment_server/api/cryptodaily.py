from db_tunnel import tunnel
from db import Database 
import pandas as pd
from bs4 import BeautifulSoup
import requests
import time 
import random
import datetime
from simpletext import send_message

def getpagecontents(pageurl):
    page = requests.get(pageurl)
    soup = BeautifulSoup(page.text, 'html.parser')
    paragraphs = soup.find_all('p')
    pagetext = ''
    for paragraph in paragraphs:
        spans = paragraph.find_all('span')
        for span in spans:
            pagetext += span.text.strip() + ' '
    time.sleep(2)
    return pagetext

def dailycoin_history(db, articlename):
    send_message('Dailycoin Historical Starting...')
    pagenum = 1
    try:
        titlestore = list(db.select_df(f'select * from {articlename}')['title'].values)
    except:
        titlestore = []
    print(titlestore)
    while True:
        try:
            titlestore = list(db.select_df(f'select * from {articlename}')['title'].values)
        except:
            titlestore = []
        url = f'https://cryptodaily.co.uk/tag/bitcoin/?page={pagenum}'
        
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
                    try:
                        db.insert_df(articledata, articlename)
                        titlestore.append(title)
                        print(f'{title} -> Inserted Properly Cryptodaily')
                    except Exception as e:
                        print(f' {e}\n[ERROR] Insert failed Cryptodaily: {title}')
        else:
            send_message('Dailycoin Historical Closed. PageNum: {pagenum}')
            print(f'[ERROR] Page error: {page.status_code} Page Num: {pagenum}')
            break
        pagenum += 1
        time.sleep(2)

def dailycoin_stream(db, articlename):
    send_message('Dailycoin Stream Starting...')
    while True:
        try:
            titlestore = list(db.select_df(f'select * from {articlename}')['title'].values)
        except:
            titlestore = []
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

        print('')
        time.sleep(15 * 60 + random.randint(-30, 30))

def main():
    with Database() as db:
        dailycoin_history(db, 'btc_article_cryptodaily')

def stream():
    with Database() as db:
        dailycoin_stream(db, 'btc_article_cryptodaily')

tunnel(stream)
