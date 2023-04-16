import time
from thoughts.sentiment_server.scraping.cryptodaily.cryptodaily import cryptodaily_history
from thoughts.sentiment_server.scraping.dailycoin.dailycoin import dailycoin_history
from  thoughts.mytools.db.db import Database
from thoughts.mytools.db.db_tunnel import tunnel
# from sentiment_server.scraping.cryptodaily.cryptodaily import cryptodaily_history
# from sentiment_server.scraping.dailycoin.dailycoin import dailycoin_history

import threading


def main():
    running = True
    dailycoinThread = None
    cryptodailyThread = None
    with Database('alterejo_dailycoin_bitcoin') as db:
        while True:
            if dailycoinThread is not None:
                if not dailycoinThread.is_alive():
                    dailycoinThread = threading.Thread(target=dailycoin_history, args=(db, 'btc_article_dailycoin'))
                    dailycoinThread.start()
            else:
                dailycoinThread = threading.Thread(target=dailycoin_history, args=(db, 'btc_article_dailycoin'))
                dailycoinThread.start()

            if cryptodailyThread is not None:
                if not cryptodailyThread.is_alive():
                    print('ran')
                    cryptodailyThread = threading.Thread(target=cryptodaily_history, args=(db, 'btc_article_cryptodaily'))
                    cryptodailyThread.start()
            else:
                print('ran')
                cryptodailyThread = threading.Thread(target=cryptodaily_history, args=(db, 'btc_article_cryptodaily'))
                cryptodailyThread.start()


            print('Sleeping for 5 minutes then will restart threads...')
            time.sleep(60 * 5)

    main()



tunnel(main)