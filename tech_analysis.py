from finta import TA
import sqlite3, math
import pandas as pd

c = sqlite3.connect('crypto_data.db')
cursor = c.cursor()

products = [
    'AAVE-USD', 
    'ALGO-USD',
    'ATOM-USD',
    'BAL-USD', 
    'BAND-USD', 
    'BCH-USD', 
    'BNT-USD', 
    'BTC-USD', 
    'CGLD-USD', 
    'COMP-USD', 
    'DASH-USD', 
    'EOS-USD', 
    #'ETC-USD', 
    'ETH-USD', 
    'FIL-USD', 
    'GRT-USD', 
    'KNC-USD', 
    'LINK-USD', 
    'LRC-USD', 
    'LTC-USD', 
    'MKR-USD', 
    'NMR-USD',
    'NU-USD',
    'OMG-USD', 
    'OXT-USD', 
    'REP-USD', 
    'REN-USD', 
    'SNX-USD', 
    'UMA-USD', 
    'UNI-USD', 
    'XLM-USD', 
    'XTZ-USD', 
    'YFI-USD', 
    'ZEC-USD', 
    'ZRX-USD'
]

def get_prices(start=1, end=6):
    print("START: BIDS")
    for product in products:
        datafile = product + '.csv'
        ohlc = pd.read_csv(datafile, index_col="date", parse_dates=True)
        p = TA.PIVOT(ohlc)
        f = "'" + product + "': ["
        prices = p.to_string().split('\n')[-1].split('  ')[1:6]
        for price in prices:
            f += str(price) + ', '
        f = f[:-2] +  '],'
        print(f)
    print("END: BIDS")
    print("")
    print("START: ASKS")
    for product in products:
        datafile = product + '.csv'
        ohlc = pd.read_csv(datafile, index_col="date", parse_dates=True)
        p = TA.PIVOT(ohlc)
        f = "'" + product + "': ["
        prices = p.to_string().split('\n')[-1].split('  ')[6:]
        for price in prices:
            f += str(price) + ', '
        f = f[:-2] +  '],'
        print(f)
    print("END: ASKS")
    #first = ohlc.groupby("close").first()
    #print(first.loc[[0.318092, 1.799902, 1.806494], ['open', 'high', 'low']])


'''e
for product in products:
    datafile = product + '.csv'
    ohlc = pd.read_csv(datafile, index_col="date", parse_dates=True)
    print(TA.PIVOT)
'''

def make_csvs():
    for product in products:
        file = open(product + '.csv', 'w')
        file.write('date,open,high,low,close,volume\n')
        for i in cursor.execute("SELECT time, open, high, low, close, volume FROM HISTORIC_PRICES WHERE COIN = '" + product + "'"):
            file.write(str(i).replace('(','').replace(')','').replace(' ','')+'\n')
        file.close()

def get_avg_for_date_range():
    for product in products:
        

#make_csvs() 
#get_prices(start=6, end=12)
