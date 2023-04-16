from sshtunnel import SSHTunnelForwarder

def tunnel(func):
    with SSHTunnelForwarder(('az1-ts100.a2hosting.com', 7822),
                                ssh_password="04x.IN)Mp6du1D",
                                ssh_username="alterejo",
                                remote_bind_address=('localhost', 5432),
                                local_bind_address=('localhost', 5432)) as tunnel:
        func()

