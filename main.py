import mysql.connector
import config as cf
from tqdm import tqdm

coredb = mysql.connector.connect(
    host=cf.core_host,
    user=cf.core_user,
    password=cf.core_pass,
    database=cf.core_db
)

localdb = mysql.connector.connect(
    host=cf.local_host,
    user=cf.local_user,
    password=cf.local_pass,
    database=cf.local_db
)

def flatten(t):
    return [item for sublist in t for item in sublist]

def RetrieveAddressesExchange():
    mycursor = coredb.cursor()
    sql = f"select address from user_coin_addresses where coin='eth'"
    mycursor.execute(sql)
    rows = mycursor.fetchall()

    if rows>0:
        rows = flatten(rows)

    return rows # type: list, len: 18072

def RetrieveAddressesLocal():
    mycursor = localdb.cursor()
    sql = f"select address from addresses"
    mycursor.execute(sql)
    rows = mycursor.fetchall()

    if rows>0:
        rows = flatten(rows)

    return rows # type: list, 

def InjectAddresses(addresses):
    mycursor = localdb.cursor()
    for addr in tqdm(addresses):
        sql = f"insert into addresses (address) values ('{addr}')"
        mycursor.execute(sql)
    localdb.commit() 


user_coin_addresses = RetrieveAddressesExchange()
local_addresses = RetrieveAddressesLocal()

n_exch = len(user_coin_addresses)
n_local = len(local_addresses)

print(f'processing {n_exch} addresses')

new_addr = [addr for addr in user_coin_addresses if addr not in local_addresses]

InjectAddresses(new_addr)

print(f'new addresses processed')