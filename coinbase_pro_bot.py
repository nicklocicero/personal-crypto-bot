import cbpro, time, env_vars, pickle, math, sqlite3, threading, datetime
from sortedcontainers import SortedDict
from decimal import Decimal

all_products = [
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
            # 'ETC-USD', 
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
            #'OXT-USD', 
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

chosen_products = [
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
    # 'NMR-USD',
    'NU-USD',
    'OMG-USD', 
    # 'OXT-USD', 
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

class myWebsocketClient(cbpro.WebsocketClient):

    def __init__(self): # add logto
        super(myWebsocketClient, self).__init__(
            products=chosen_products, channels=['level2'])
        self._book = {}
        self.auth_client = cbpro.AuthenticatedClient(env_vars.API_KEY, env_vars.API_SECRET, env_vars.API_PASS)
        self._usd_balance = 0
        self._usdc_balance = 0
        self._orders_made = []
        self._order = True
        self._funds_total = 33000.46
        self.min_asks = {}
        self.max_bids = {}
        self.death_bids = {}
        self.cursor = self.create_cursor('crypto_data.db')
        self._balance_spread = [0, 0, 0, 0, 0]
        self.tier_percent = [0, 0, 0, 0, 0]
        # 'BAT-USDC', 'CVC-USDC', 'DNT-USDC', 'LOOM-USDC', 'MANA-USDC', 'GNT-USDC'
        self.products = chosen_products.copy()
        self.all_products = all_products.copy()
        self._holdings = {}
        self.reduce_factor = 1

        self.all_death_bids = {
            'AAVE-USD': [437.288000, 424.820000, 404.747000, 392.279000, 379.811],
            'ALGO-USD': [1.160667, 1.136333, 1.092467, 1.068133, 1.0438],
            'ATOM-USD': [20.461000, 19.702000, 18.281000, 17.522000, 16.763],
            'BAL-USD': [49.703333, 46.686667, 44.373333, 41.356667, 38.34000],
            'BAND-USD': [14.864233, 14.386067, 13.848933, 13.370767, 12.8926],
            'BCH-USD': [538.906667, 530.283333, 514.976667, 506.353333, 497.73],
            'BNT-USD': [9.114800, 8.850900, 8.619700, 8.355800, 8.0919],
            'BTC-USD': [53899.130000, 52862.260000, 50808.140000, 49771.270000, 48734.40],
            'CGLD-USD': [3.955900, 3.881500, 3.775600, 3.701200, 3.6268],
            'COMP-USD': [492.290000, 480.080000, 458.390000, 446.180000, 433.97],
            'DASH-USD': [234.972000, 223.945000, 203.674000, 192.647000, 181.620],
            'EOS-USD': [4.068667, 3.951333, 3.732667, 3.615333, 3.498],
            'ETH-USD': [1848.083333, 1823.906667, 1775.823333, 1751.646667, 1727.47],
            'FIL-USD': [41.901833, 41.265967, 40.542533, 39.906667, 39.2708],
            'GRT-USD': [2.069533, 1.931667, 1.743133, 1.605267, 1.4674],
            'KNC-USD': [2.198900, 2.077800, 1.979000, 1.857900, 1.7368],
            'LINK-USD': [31.298177, 30.596353, 29.680827, 28.979003, 28.27718],
            'LRC-USD': [0.609300, 0.588600, 0.557200, 0.536500, 0.5158],
            'LTC-USD': [199.396667, 193.593333, 183.206667, 177.403333, 171.60],
            'MKR-USD': [2228.097467, 2181.694933, 2098.389867, 2051.987333, 2005.5848],
            'NMR-USD': [41.586900, 40.203800, 38.737300, 37.354200, 35.9711],
            'NU-USD': [0.706033, 0.693067, 0.677133, 0.664167, 0.6512],
            'OMG-USD': [5.349100, 5.165300, 4.908100, 4.724300, 4.5405],
            'REP-USD': [32.730000, 31.810000, 30.510000, 29.590000, 28.67],
            'REN-USD': [1.301867, 1.219033, 1.096867, 1.014033, 0.9312],
            'SNX-USD': [22.247067, 21.745433, 20.868067, 20.366433, 19.8648],
            'UMA-USD': [27.198333, 25.507667, 23.705333, 22.014667, 20.324],
            'UNI-USD': [33.335233, 31.897167, 30.915433, 29.477367, 28.0393],
            'XLM-USD': [0.426669, 0.418364, 0.404694, 0.396389, 0.388083],
            'XTZ-USD': [4.468300, 4.347500, 4.229200, 4.108400, 3.9876],
            'YFI-USD': [39302.813333, 38298.256667, 37596.723333, 36592.166667, 35587.61],
            'ZEC-USD': [134.773333, 131.046667, 124.273333, 120.546667, 116.82],
            'ZRX-USD': [1.448087, 1.419582, 1.364497, 1.335992, 1.307488]
            # 'BAT-USDC' : 0.245,
            # 'BTC-USDC' : 0.00,
            # 'CVC-USDC' : 0.15127,
            # 'DNT-USDC' : 0.085,
            # 'ETH-USDC' : 0.00,
            # 'GNT-USDC' : 0.72,
            # 'LOOM-USDC' : 0.48,
            # 'MANA-USDC' : 0.232,
            # 'ZEC-USDC' : 0.00,
        }

        self._ask_prices = {
            'AAVE-USD': [401.365667, 428.222333, 445.908667, 463.595],
            'ALGO-USD': [1.210000, 1.330000, 1.410000, 1.4900],
            'ATOM-USD': [21.075000, 23.315000, 24.781000, 26.247],
            'BAL-USD': [39.136360, 41.012720, 42.304260, 43.59580],
            'BAND-USD': [13.856200, 14.772100, 15.611000, 16.4499],
            'BCH-USD': [531.720000, 558.410000, 577.950000,  597.49],
            'BNT-USD': [7.240967, 7.668033, 8.056167,  8.4443],
            'BTC-USD': [50943.900000, 53519.280000, 55240.230000, 56961.18],
            'CGLD-USD': [4.169333, 4.475167, 4.648833, 4.8225],
            'COMP-USD': [496.640000, 523.580000, 544.190000, 564.80],
            'DASH-USD': [225.478000, 239.673000, 249.346000, 259.019],
            'EOS-USD': [3.903333, 4.058667, 4.193333, 4.328],
            'ETH-USD': [1608.440000, 1677.190000, 1729.380000, 1781.57],
            'FIL-USD': [42.715533, 44.694067, 46.166933, 47.6398],
            'GRT-USD': [2.000500, 2.145200, 2.250400, 2.3556],
            'KNC-USD': [2.164133, 2.317067, 2.514133, 2.7112],
            'LINK-USD': [29.828277, 31.978413, 33.380987, 34.78356],
            'LRC-USD': [0.581933, 0.620467, 0.642733, 0.6650],
            'LTC-USD': [191.060000, 201.530000, 208.560000, 215.59],
            'MKR-USD': [2274.644400, 2353.692900, 2467.833400, 2581.9739],
            'NMR-USD': [42.250733, 45.353667, 47.224033,  49.0944],
            'NU-USD': [0.717100, 0.758000, 0.786300, 0.8146],
            'OMG-USD': [4.916367,  5.174133,  5.374467,  5.5748],
            'OXT-USD': [0.512067, 0.541733, 0.563467, 0.5852],
            'REP-USD': [31.170000, 32.330000, 33.060000, 33.79],
            'REN-USD': [1.125467, 1.204933, 1.259867, 1.3148],
            'SNX-USD': [23.535633, 25.594867, 26.789733, 27.9846],
            'UMA-USD': [22.193000, 23.471000, 24.495000, 25.519],
            'UNI-USD': [30.425533, 32.615667, 35.453433, 38.2912],
            'XLM-USD': [0.425394, 0.439095, 0.450593, 0.462091],
            'XTZ-USD': [3.900367, 4.140933, 4.311867, 4.4828],
            'YFI-USD': [34096.416667, 35851.083333, 37145.426667, 38439.77],
            'ZEC-USD': [128.896667, 135.793333, 141.606667, 147.42],
            'ZRX-USD': [1.461787, 1.551317, 1.604469, 1.657621]
        }

        self._coins_balances_avg_prc = {}
        self._coins_target_buy_and_sell = {}

    def create_cursor(self, db_file):
        """ create a database connection to the SQLite database
            specified by db_file
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = sqlite3.connect(db_file, timeout=10, isolation_level='IMMEDIATE')
            cursor = conn.cursor()
            return cursor
        except Error as e:
            print(e)

        return cursor

    def on_open(self):
        self.set_usd_usdc()
        self.make_dicts()
        self.get_holdings()
        #self.make_db()
        print("Subscribed to order book!")
        if self._order:
            print("Buying mode ON.")
        else:
            print("Buying mode OFF.")

    def on_close(self):
        print("\n-- OrderBook Socket Closed! --")

    def make_dicts(self):
        self.min_asks["Averages"] = [1000, 1000, 1000, 1000, 1000]
        for product in self.products:
            self.min_asks[product] = 100000000.0
            self.max_bids[product] = 0.0
            self.death_bids[product] = self.all_death_bids[product].copy()

    def get_holdings(self):
        accts = self.auth_client.get_accounts()
        for acct in accts:
            if not acct["currency"] == "USD":
                self._holdings[acct["currency"] + "-USD"] = {"balance": float(acct["balance"])}
        
    
    def get_pending_orders(self):
        pending_orders = self.auth_client.get_orders()
    
    def price_pending_orders(self):
        ""
    
    def make_db(self):
    
        make_historic_prices_table = '''CREATE TABLE IF NOT EXISTS HISTORIC_PRICES(
            coin text NOT NULL,
            time timestamp NOT NULL,
            low float NOT NULL,
            high float NOT NULL,
            open float NOT NULL,
            close float NOT NULL,
            volume float,
            PRIMARY KEY(coin, time)
        )'''

        historic_pricing_data_insert = '''INSERT OR REPLACE INTO historic_prices(coin, time, low, high, open, close, volume) VALUES(?, ?, ?, ?, ?, ?, ?);'''

        conn = None
        try:
            conn = sqlite3.connect('crypto_data.db', timeout=10, isolation_level='IMMEDIATE')
            cursor = conn.cursor()
        except Error as e:
            print(e)

        cursor.execute(make_historic_prices_table)

        try:
            for product in self.all_products:
                time.sleep(2)
                prices = self.auth_client.get_product_historic_rates(product, granularity=86400)
                stats = self.auth_client.get_product_24hr_stats(product_id=product)
                insert_list = []
                #insert_list = [[product, datetime.datetime.now(), stats["low"], stats["high"], stats["open"], stats["last"], stats["volume"]]]
                for mark in prices:
                    insert_list.append([product, str(datetime.datetime.fromtimestamp(int(mark[0]))), mark[1], mark[2], mark[3], mark[4], mark[5]])
                cursor.executemany(historic_pricing_data_insert, insert_list)
                time.sleep(1)
        except EOFError as e:
            print(e)
        conn.commit()
        conn.close()

    def set_asks(self, product_id, price, asks):
        self._book[product_id]["asks"][price] = asks
        if self.min_asks[product_id] > price:
            self.min_asks[product_id] = price

    def remove_asks(self, product_id, price):
        self._book[product_id]["asks"].pop(price)
        if self.min_asks[product_id] >= price:
            self.min_asks[product_id] = min(self._book[product_id]["asks"].keys())
            
    def set_bids(self, product_id, price, bids):
        self._book[product_id]["bids"][price] = bids
        if self.max_bids[product_id] < price:
            self.max_bids[product_id] = price

    def remove_bids(self, product_id, price):
        self._book[product_id]["bids"].pop(price)
        if self.max_bids[product_id] <= price:
            self.max_bids[product_id] = max(self._book[product_id]["bids"].keys())
            
    
    def get_tier(self):
        for i in range(len(self._balance_spread)):
            if self._balance_spread[i] * self._funds_total < self._usd_balance:
                return i
        return None
    
    def set_usd_usdc(self):
        self._usd_balance = math.floor(float(self.auth_client.get_account('823b7cde-4907-4174-8a57-43877672390f')["balance"])*100)/100.0 # usd
        # self._usdc_balance = math.floor(float(self.auth_client.get_account('961209d2-2ec3-4be1-a6f3-b300db9c428a')["balance"])*100)/100.0 # usdc
    
    def update_dollar_balance(self):
        for coin in self._coins_balances_avg_prc.keys():
            self._coins_balances_avg_prc[coin]["USD Value"] = self._coins_balances_avg_prc[coin]["balance"] * self.min_asks[coin]
    
    def make_order(self, product_id, tier):
        # TODO: Calculate the right market size.
        if self.death_bids[product_id][tier] >= self.min_asks[product_id] and self.min_asks['Averages'][tier] < 1:
            # if self.tier_percent[tier] >= self.min_asks[product_id]:
            if self._usd_balance < 35.0:
                _order_id = self.auth_client.place_market_order(product_id=product_id, side="buy", funds=(self._usd_balance - 1))["id"]
            else:
                if tier == 0:
                    _order_id = self.auth_client.place_market_order(product_id=product_id, side="buy", funds=85.00)["id"]
                elif tier == 1:
                    _order_id = self.auth_client.place_market_order(product_id=product_id, side="buy", funds=85.0)["id"]
                else:
                    _order_id = self.auth_client.place_market_order(product_id=product_id, side="buy", funds=85.0)["id"]
            order = self.auth_client.get_order(_order_id)
            no_status = True
            while no_status:
                if "status" in order.keys():
                    no_status = False
                    while order["status"] != "done":
                        time.sleep(0.501)
                        order = self.auth_client.get_order(_order_id)
                else:
                    time.sleep(0.501)
                    order = self.auth_client.get_order(_order_id)
            # TODO: Setup logs.
            msg = time.strftime("%H:%M:%S", time.localtime()) + " - " + order["filled_size"] + " of " + order["product_id"] + " for " + order["funds"] + " at " + str(float(order["funds"])/float(order["filled_size"])) + " per coin."
            print(msg)
            self._orders_made.append(msg)
        self.set_usd_usdc()    

    def try_order(self, print_check):
        tier = self.get_tier()
        for product_id in self.death_bids.keys():
            if tier is not None and product_id != 'Averages':
                if self._order == True:
                    self.make_order(product_id, tier)
            time.sleep(0.501)
        if print_check % 1 == 0:
            if tier is None:
                print("No cash left.", self._usd_balance)
                return
            coins = {}
            print("=================================================================================")
            print("|   Coin    |   Ask    |       Tier       |       Tier       |       Tier       |")
            avg = [0 for i in range(len(self.death_bids["GRT-USD"]))]
            avg_d = 0
            for key in self.min_asks.keys():
                if type(self.min_asks[key]) == list:
                    continue
                avg_d += 1
                key_p = "| " + key + " "
                while len(key_p) < 12:
                    key_p += " "
                key_p += "| "
                min_ask_p = str(math.floor(self.min_asks[key]*10000)/10000.0)
                while len(min_ask_p) < 8:
                    min_ask_p += " "
                death_bid_p = ""
                p_key_use = True
                for t in range(0, len(self.death_bids["GRT-USD"])):
                    death_bid = self.death_bids[key][t] * self.reduce_factor
                    p = math.floor(10000 - (death_bid*self.reduce_factor/self.min_asks[key])*10000)/100.0
                    if p_key_use:
                        p_key = p
                        p_key_use = False
                    d = str(math.floor(death_bid*self.reduce_factor*10000)/10000.0)
                    d = d[:7] + " "
                    while len(d) < 9:
                        d += "-"
                    d += "- " + str(p)
                    while len(d) < 16:
                        d += " "
                    death_bid_p += " | " + d
                    avg[t] += p
                coins[p_key] = key_p + min_ask_p[:7] + " " + death_bid_p + " |"
            for coin in sorted(coins.keys(), reverse=True):
                print(coins[coin])
            print(self.print_avg(tier, avg, avg_d))
            self.print_end(tier)

    def make_sell(self):
        """
        Sells crypto for cash.
        """

    def try_sell(self, print_check):
        for holding in self._holdings.keys():
            if holding == "USDC-USD":
                continue
            if holding in chosen_products and self._holdings[holding]["balance"] > 0:
                time.sleep(.5)
                if self.min_asks[holding] < 80000:
                    amount = 100/self.min_asks[holding]
                    if holding in ["COMP-USD"]:
                        continue
                    _order_id = self.auth_client.place_market_order(product_id=holding, side="sell", size=math.floor(amount*1000.0)/1000.0)
                    print(holding, amount)
                    if holding in ["EOS-USD", "XTZ-USD", "OMG-USD"]:
                        continue
                    if "id" not in _order_id.keys():
                        print(_order_id)
                        if _order_id["message"] == "size is too accurate. Smallest unit is 0.10000000":
                            _order_id = self.auth_client.place_market_order(product_id=holding, side="sell", size=math.floor(amount*10.0)/10.0)
                        elif _order_id["message"] == "size is too accurate. Smallest unit is 0.01000000":
                            _order_id = self.auth_client.place_market_order(product_id=holding, side="sell", size=math.floor(amount*100.0)/100.0)
                        elif _order_id["message"] == "size is too accurate. Smallest unit is 0.00010000":
                            _order_id = self.auth_client.place_market_order(product_id=holding, side="sell", size=math.floor(amount*1000.0)/1000.0)
                        elif _order_id["message"] == "size is too accurate. Smallest unit is 1.00000000":
                            _order_id = self.auth_client.place_market_order(product_id=holding, side="sell", size=math.floor(amount))
                        elif _order_id["message"] == 'size is too small. Minimum size is 0.00001000':
                            continue
                        elif _order_id["message"] == 'Insufficient funds':
                            continue
                    order = self.auth_client.get_order(_order_id["id"])
                    no_status = True
                    while no_status:
                        if "status" in order.keys():
                            no_status = False
                            while order["status"] != "done":
                                time.sleep(0.501)
                                print(_order_id)
                                order = self.auth_client.get_order(_order_id["id"])
                        else:
                            time.sleep(0.501)
                            if type(_order_id) is dict:
                                continue
                            print(_order_id)
                            order = self.auth_client.get_order(_order_id["id"])
                    print(order)
                    msg = time.strftime("%H:%M:%S", time.localtime()) + " - " + order["filled_size"] + " of " + order["product_id"] + " for " + order["executed_value"] + " at " + str(float(order["executed_value"])/float(order["filled_size"])) + " per coin."
                    print(msg)
            # print(self._holdings[holding])
        self.get_holdings()
        self.set_usd_usdc()
        print(self._usd_balance)

    def print_avg(self, tier, avg, avg_d):
        p = "| Average   | " + "-" * 8 + " | "
        c = ""
        tier = 0
        for a in avg:
            percent = math.floor((math.floor(a*1000)/1000.0)*100/float(avg_d))/100.0
            self.min_asks["Averages"][tier] = percent
            c = 10 * "-" + " " + str(math.floor((math.floor(a*1000)/1000.0)*100/float(avg_d))/100.0)
            while len(c) < 16:
                c += " "
            p += c + " | "
            tier += 1
        return p
        
    
    def print_end(self, tier):
        money_left = str(math.floor((self._usd_balance - self._funds_total * self._balance_spread[tier])*100)/100.0)
        print("=================================================================================")
        print("| " + time.strftime("%H:%M:%S", time.localtime()) + ": Tier", str(tier) + " w/ cash balance @ $" + str(self._usd_balance) + "." + 24 * " ", "        |")
        print("| $" + money_left + " left to spend.                                                     " + ((8 - len(money_left)) * " ") + " |")
        print("=================================================================================")
        if len(self._orders_made) == 0:
            print("| No orders made.                                                               |")
        else:
            for order in self._orders_made:
                print(order)
    
    def on_message(self, message):
        msg_type = message['type']
        product_id = message.get("product_id", "")

        if msg_type == 'snapshot':
            self._book[product_id] = {"asks": {}, "bids": {}} # SortedDict()}
            for ask in message["asks"]:
                self._book[product_id]["asks"][float(ask[0])] = ask[1]
            for bid in message["bids"]:
                self._book[product_id]["bids"][float(bid[0])] = bid[1]
            self.min_asks[product_id] = min(self._book[product_id]["asks"].keys())
            
        elif msg_type == "l2update":
            for change in message["changes"]:
                if change[0] == "sell":
                    if float(change[2]) == 0:
                        self.remove_asks(product_id, float(change[1]))
                    else:
                        self.set_asks(product_id, float(change[1]), change[2])
                elif change[0] == "buy":
                    if float(change[2]) == 0:
                        self.remove_bids(product_id, float(change[1]))
                    else:
                        self.set_bids(product_id, float(change[1]), change[2])
            if "BTC-USD" in self.max_bids.keys():
                print("buy", self.max_bids["BTC-USD"])
            self.update_dollar_balance()

wsClient = myWebsocketClient()
wsClient.daemon = True
wsClient.start()
a = 0

while True:
    a += 1
    time.sleep(.1)
    wsClient.try_order(a)

print("finished while")