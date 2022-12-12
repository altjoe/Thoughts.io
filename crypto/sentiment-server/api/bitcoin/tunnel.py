from sshtunnel import SSHTunnelForwarder
import datetime 
import time

def open_tunnel():
    while True:
        with SSHTunnelForwarder(('az1-ts100.a2hosting.com', 7822),
                                ssh_password="04x.IN)Mp6du1D",
                                ssh_username="alterejo",
                                remote_bind_address=('localhost', 5432),
                                local_bind_address=('localhost', 5432)) as tunnel:
            while True:
                print(f'Connection open {datetime.datetime.now()}')
                time.sleep(5)
        print(f'Connection reopening in 5 seconds {datetime.datetime.now()}')
        time.sleep(5)

