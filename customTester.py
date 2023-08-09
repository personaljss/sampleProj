import pandas as pd
from tqdm import tqdm
from models import *
import numpy as np
import time

class Order():
    """
    This class represents both a market and limit order. It does not capture other order types for now for simplicity
    the slippage for limit orders is always 0
    """   
    _order_id_counter = 0  # This is the class-level attribute (static variable) for counting order IDs.

    def __init__(self, ticker, size, side, time, price,slippage=0):
        self.ticker = ticker
        self.side = side
        self.size = size
        self.type = 'market' # this can be market, limit or delete
        self.bist_time = self._adjust_time_with_latency(pd.to_datetime(time))
        self.slippage = slippage
        self.id = self._generate_id()  # Assign a unique ID to the order when it's created.
        self.price=price
        self.sl=0
        self.tp=np.Infinity
        self.exec_data=[]#this list will hold the dicts with keys price, time, volume to handle partial execution
    
    @property
    def waiting_volume(self):
        lots=self.size
        for data in self.exec_data:
            lots-=data['volume']
        return lots

    @staticmethod
    def _get_latency():
        latency = 0
        while latency <= 0:
            latency = np.random.normal(20, 10)
        return pd.Timedelta(nanoseconds=int(latency))
    
    def execute(self, price, volume, time):
        wv=self.waiting_volume
        if volume>wv:
            volume=wv
        self.exec_data.append({'price':price, 'volume' : volume, 'time':time})

    def _adjust_time_with_latency(self, time):
        return time + self._get_latency()

    @classmethod
    def _generate_id(cls):
        cls._order_id_counter += 1
        return str(cls._order_id_counter)

        
class Trade():
    
    _trade_id_counter = 0

    def __init__(self, ticker,side,size,price,time,sl,tp,parent_id=None):
        self.ticker = ticker
        self.side = side
        self.price = price
        self.size = size # trade opening size
        self.time = time
        self.sl = sl
        self.tp = tp
        self.id = self._generate_trade_id()  # Assign a unique ID to the trade when it's happend
        #this list will hold the dictionaries with keys price,volume,time. It is to keep track of partial executions while closing       
        self.close_data=[]
    
    @property
    def active_size(self):
        lots=self.size
        for data in self.close_data:
            lots-=data['volume']
        return lots
    
    def close(self, price, volume, time):
        self.close_data.append({'price':price, 'volume' : volume, 'time':time})

    @classmethod
    def _generate_trade_id(cls):
        cls._trade_id_counter += 1
        return str(cls._order_id_counter)
    
class Engine():
    def __init__(self, data : pd.DataFrame):
        self.strategy : Strategy = None
        self.data=data
        self.current_time = None
        
    def add_data(self, data: pd.DataFrame):
        #checking the validity of the data
        required_columns = {
            #"Date",
            "Asset",
            "bid1qty",
            "bid1px",
            "bid2qty",
            "bid2px",
            "bid3qty",
            "bid3px",
            "ask1px",
            "ask1qty",
            "ask2px",
            "ask2qty",
            "ask3px",
            "ask3qty",
            "Mold Package"
        }

        missing_columns = required_columns - set(data.columns)

        if missing_columns:
            raise ValueError(f"Missing columns: {', '.join(missing_columns)}")

        self.data = data

        
    def add_strategy(self, strategy):
        # Add a strategy to the engine
        self.strategy = strategy
        self.strategy.add_data(self.data)
    
    def run(self):
        if not self.strategy:
            raise ValueError("No strategy has been added. Use the add_strategy method to add one.")
        if self.strategy.data is None or self.strategy.data.empty:
            raise ValueError("No data has been added to the strategy. Use the add_data method to add one.")
        initial_balance=self.strategy.cash
        for time in self.data.index:
            #synchronising the times
            self.current_time = time
            self.strategy.current_time = time
            # adjust current positions
            self._check_trades()
            # fill orders from previus period
            self._fill_orders()
            
            # Run the strategy on the current bupdate
            self.strategy.on_update()
        
        last_bid=self.data[self.data['bid1px'] != 0]['bid1px'].iloc[-1]
        last_ask=self.data[self.data['ask1px'] != 0]['ask1px'].iloc[-1]
        hold_asset_value=0
        
        for pos in self.strategy.current_positions.values():
            if pos.side=='B':
                hold_asset_value+=pos.active_size*last_ask
            else:
                hold_asset_value-=pos.active_size*last_bid
        return f"initial balance: {initial_balance}, final portfolio: {self.strategy.cash} cash and {hold_asset_value} assets"
            
    def _fill_orders(self):
        """this method fills buy and sell orders, creating new trade objects and adjusting the strategy's cash balance.
        Conditions for filling an order:
        - If we're buying, our cash balance has to be large enough to cover the order.
        - If we are selling, no constarint for the sake of simplicity, no margin calculations are needed now.   
        """
        for order in self.strategy.waiting_orders.values():
            #checking the order time to prevent look ahead. Because there is a latency when sending orders to the market
            if order.bist_time <= self.current_time:
                #if it is a sell
                if order.side=='S':
                    offered_price=self.data.loc[self.current_time]['bid1px']
                    #we are handling the slippage
                    if offered_price>=order.price-order.slippage:
                        #the order should be executed by the minimum qty of ask1qty and order.size
                        volume = min(self.data.loc[self.current_time]['ask1qty'], order.size)
                        self.strategy.position_open(order, offered_price, volume, self.current_time)
                    else:
                        #the order should be deleted if it is a market order
                        if order.type=='market':
                            self.strategy.order_delete(order)
                #if it is a buy                   
                elif order.side=='B':
                    #we are handling the slippage
                    offered_price=self.data.loc[self.current_time]['ask1px']
                    if offered_price>=order.price+order.slippage:
                        #the order should be executed by the minimum qty of ask1qty and order.size
                        volume = min(self.data.loc[self.current_time]['ask1qty'], order.size)
                        self.strategy.position_open(order, offered_price, volume, self.current_time)
                    else:
                        #the  order should be deleted if it is a market order
                        if order.type=='market':
                            self.strategy.order_delete(order)
    
    def _check_trades(self):
        """
        This method is responsible for managing the current trades of a strategy.
        """
        for trade in self.strategy.current_positions.values():
            #check for tp's and sl's
            if trade.side=='B':
                offered_price=self.data.loc[self.current_time]['ask1px']
                available_volume=self.data.loc[self.current_time]['ask1qty']
                
                if trade.sl<=offered_price or trade.tp>=offered_price:
                    self.strategy.position_close(trade,offered_price,available_volume,self.current_time)
                
            elif trade.side=='S':
                offered_price=self.data.loc[self.current_time]['bid1px']
                available_volume=self.data.loc[self.current_time]['bid1qty']
                
                if trade.sl>=offered_price or trade.tp<=offered_price:
                    self.strategy.position_close(trade,offered_price,available_volume,self.current_time)                
                
            

class Strategy():
    def __init__(self, ticker, initial_cash=100_000):
        self.current_time = None
        self.data = None
        self.cash = initial_cash
        self.orders = {} # this dict will hold the current waiting orders, keys are order id's
        self.positions = {} #t his dict will hold the trade objects representing executed orders, keys are trade id's
        self.ticker=ticker
    
    #this method will return the limit orders that are not executed yet
    @property
    def waiting_orders(self):
        wo={}
        for order in self.orders.values():
            if order.waiting_volume>0 and order.type=='limit':
                wo[order.id]=order
        return wo
    
    #this method will return the current positions
    @property
    def current_positions(self):
        cp={}
        for pos in self.positions.values():
            if pos.active_size>0:
                cp[pos.id]=pos
        return cp

    def order_send(self,order):        
        self.orders[order.id]=order
    
    def order_delete(self, order):
        del self.orders[order.id]
    
    def position_open(self, order: Order, price, volume, time):
        #create a new trade, add it to the open trades
        trade = Trade(self.ticker,order.side, volume, price, time, order.sl, order.tp)
        self.positions[trade.id] = trade
        order.execute(price,volume,time) 
        #adjust the cash balance
        if order.side=='S':
            self.cash+=volume*price
        elif order.side=='B':
            self.cash-=volume*price
        

    def position_close(self, trade: Trade, price, volume_available, time):
        #add the trade to closed trades, delete it from the current positions 
        # if it is partially executed, add a new trade to the positions with the remaining lots
        trade.close_time=time
        trade.closed_price=price
        trade.close(price,volume_available,time)
        #Finally adjust the cash balance
        if trade.side=='B':
            self.cash+=(price-trade.price)*trade.closed_size
        elif trade.size=='S':
            self.cash+=(trade.price-price)*trade.closed_size           
    
    @property
    def position_size(self):
        return sum([t.active_size for t in self.positions.values()])
        
    def on_update(self):
        """
        This method will be overriden by our strategies.
        """
        pass

class OBIStrategy(Strategy):
    def __init__(self, ticker, obi_threshold, initial_cash=100_000, slippage=0.01):
        super().__init__(ticker, initial_cash)
        self.data = None
        self.obi_threshold = obi_threshold
        self.slippage = slippage  # Slippage for market orders

    def add_data(self, data : pd.DataFrame):
        # copying the data because when we do not do that, there may be warnings
        self.data=data.copy()
        self.calculate_obi()  # Calculate OBI within constructor
    
    def calculate_obi(self):
        self.data.loc[:, 'OBI'] = (self.data['bid1qty'] - self.data['ask1qty']) / (self.data['bid1qty'] + self.data['ask1qty'])
        self.data.loc[:, 'signal'] = 0
        self.data.loc[self.data['OBI'] > self.obi_threshold, 'signal'] = 1
        self.data.loc[self.data['OBI'] < -self.obi_threshold, 'signal'] = -1

    def close_positions(self, direction, price, volume,time):
        for trade in self.positions.values():
            if trade.direction == direction:
                self.position_close(trade,price,volume,time)


    def on_update(self):
        obi_signal = self.data.loc[self.current_time, 'signal']
        print(obi_signal)
        # Buy condition
        if obi_signal == 1:
            # Close any open sell trades
            price = self.data.loc[self.current_time, 'bid1px']
            volume=self.data.loc[self.current_time,'bid1qty']
            self.close_positions('S',price,volume,self.current_time)
            # Place buy order
            order = Order(self.ticker, 100, 'B', self.current_time, price,self.slippage)
            order.tp = price + 0.02
            order.sl = price - 0.02
            self.order_send(order)

        # Sell condition
        elif obi_signal == -1:
            # Close any open buy trades
            price = self.data.loc[self.current_time, 'ask1px']
            volume=self.data.loc[self.current_time,'ask1qty']
            # Close any open sell trades
            self.close_positions('B',price,volume,self.current_time)
            # Place sell order
            price = self.data.loc[self.current_time, 'bid1px']
            order = Order(self.ticker, 100, 'S', self.current_time, price,self.slippage)
            order.tp = price - 0.02
            order.sl = price + 0.02
            self.order_send(order)

if __name__=="__main__":
    data_processor = DataProcessor('AKBNK.E.csv')
    #creating df's for limit order book and executions
    start_time = time.time()
    lob_snaps, execs = data_processor.lob_dfs()
    end_time = time.time()
    print(f"It took {end_time - start_time} seconds to retrieve the data.")    
    
    data = lob_snaps[lob_snaps.index < lob_snaps.index[0] + pd.Timedelta(minutes=120)]
    engine=Engine(data)
    strategy=OBIStrategy("AKBNK",0.7,)
    engine.add_strategy(strategy)

    start_time = time.time()

    result = engine.run()

    end_time = time.time()

    print(f"It took {end_time - start_time} seconds to run the test.")

    print(result)

