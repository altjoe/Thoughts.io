from db_tunnel import tunnel
from db import Database 
import pandas as pd
from bs4 import BeautifulSoup
import requests
import time 
import random

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
    pagenum = 2
    try:
        titlestore = list(db.select_df(f'select * from {articlename}')['title'].values)
    except:
        titlestore = []
    print(titlestore)
    while True:
        url = f'https://dailycoin.com/bitcoin/?page={pagenum}'
        
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
                if title not in titlestore:
                    try:
                        db.insert_df(articledata, articlename)
                        titlestore.append(title)
                        print(f'{title} -> Inserted Properly')
                    except Exception as e:
                        print(f' {e}\n[ERROR] Insert failed: {title}')

                    
            
        else:
            print(f'[ERROR] Page error: {page.status_code} Page Num: {pagenum}')
            break
        pagenum += 1
        time.sleep(2)

def dailycoin_stream(db, articlename):
    while True:
        try:
            titlestore = list(db.select_df(f'select * from {articlename}')['title'].values)
        except:
            titlestore = []
        url = f'https://dailycoin.com/bitcoin/'
        
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
                if title not in titlestore:
                    try:
                        db.insert_df(articledata, articlename)
                        titlestore.append(title)
                        print(f'{title} -> Inserted Properly')
                    except Exception as e:
                        print(f' {e}\n[ERROR] Insert failed: {title}')

                    
            
        else:
            print(f'[ERROR] Page error: {page.status_code}')

        time.sleep(15 * 60 + random.randint(-30, 30))

def main():
    with Database() as db:
        dailycoin_history(db, 'btc_article_dailycoin')

def stream():
    with Database() as db:
        dailycoin_stream(db, 'btc_article_dailycoin')

tunnel(stream)