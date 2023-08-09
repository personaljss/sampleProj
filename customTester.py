import pandas as pd
from tqdm import tqdm
from models import *
import numpy as np

class Order():
    """
    This class represents both a market and limit order. It does not capture other order types for now for simplicity
    the slippage for limit orders is always 0
    """   
    _order_id_counter = 0  # This is the class-level attribute (static variable) for counting order IDs.

    def __init__(self, ticker, size, side, time, slippage=0):
        self.ticker = ticker
        self.side = side
        self.size = size
        self.type = 'market'
        self.bist_time = self._adjust_time_with_latency(pd.to_datetime(time))
        self.slippage = slippage
        self.id = self._generate_id()  # Assign a unique ID to the order when it's created.
        self.sl=0
        self.tp=np.Infinity

    @staticmethod
    def _get_latency():
        latency = 0
        while latency <= 0:
            latency = np.random.normal(20, 10)
        return pd.Timedelta(nanoseconds=int(latency))

    def _adjust_time_with_latency(self, time):
        return time + self._get_latency()

    @classmethod
    def _generate_id(cls):
        cls._order_id_counter += 1
        return str(cls._order_id_counter)

        
class Trade():
    def __init__(self, ticker,side,size,price,time,sl,tp,parent_id=None):
        self.ticker = ticker
        self.side = side
        self.price = price
        self.size = size
        self.time = time
        self.sl = sl
        self.tp = tp
        self.id = self._generate_trade_id()  # Assign a unique ID to the order when it's created.
        self.close_time=None
        self.closed_size=0
        self.closed_price=0
        #this is an attribute to keep track of the volume when partial executions happen.
        # I think it is not really practical however I added this to be realistic
        self.parent_id=parent_id
    
    @property
    def active_size(self):
        return self.size-self.closed_size

    @classmethod
    def _generate_trade_id(cls):
        cls._order_id_counter += 1
        return str(cls._order_id_counter)
    
class Engine():
    def __init__(self,):
        self.strategy : Strategy = None
        self.data : pd.DataFrame= None
        self.current_time = None
        
    def add_data(self, data: pd.DataFrame):
        #checking the validity of the data
        required_columns = {
            "Date",
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
    
    def run(self):
        if not self.strategy:
            raise ValueError("No strategy has been added. Use the add_strategy method to add one.")        
        initial_balance=self.strategy.cash
        # We need to preprocess a few things before running the backtest
        self.strategy.data = self.data
        
        for time in tqdm(self.data.index):
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
        for id, pos in self.strategy.positions:
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
        for id, order in self.strategy.orders:
            #checking the order time to prevent look ahead. Because there is a latency when sending orders to the market
            if order.bist_time <= self.current_time:
                #if it is a sell
                if order.side=='S':
                    #we are handling the slippage
                    offered_price=self.data.loc[self.current_time]['bid1px']
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
        This method is responsible for managing the current trades of the strategy.
        """
        for id, trade in self.strategy.positions:
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
        self.orders = {}
        self.positions = {}
        self.closed_trades = {}
        self.ticker=ticker

    def order_send(self,order):        
        self.orders[order.id]=order
    
    def order_delete(self, order):
        del self.orders[order.id]
    
    def position_open(self,order: Order,price, volume, time):
        #create a new trade, add it to the open trades
        trade = Trade(self.ticker,order.side, volume, price, time,order.sl, order.tp)
        self.positions[trade.id] = trade
        #delete the order from the orders and handling the partial execution
        if order.size==trade.size:
            del self.orders[order.id]
        else:
            self.orders[order.id].size-=volume
        #adjust the cash balance
        if order.side=='S':
            self.cash+=volume*price
        else:
            self.cash-=volume*price
        

    def position_close(self, trade: Trade, price, volume_available, time):
        #add the trade to closed trades, delete it from the current positions 
        # if it is partially executed, add a new trade to the positions qith the remaining lots
        trade.close_time=time
        trade.closed_price=price
        if trade.active_size<=volume_available:
            trade.closed_size=trade.active_size
        else:
            trade.closed_size+=volume_available
            #creating a new trade object to keep track of the remaining lots
            new_trade=Trade(trade.ticker,trade.side,trade.active_size,trade.price,trade.time,trade.sl,trade.tp,trade.id)
            self.positions[new_trade.id]=new_trade
        del self.positions[trade.id]
        self.closed_trades[trade.id]=trade
        #Finally adjust the cash balance
        if trade.side=='B':
            self.cash+=(price-trade.price)*trade.closed_size
        else:
            self.cash+=(trade.price-price)*trade.closed_size           
    
    @property
    def position_size(self):
        return sum([t.size for t in self.trades])
        
    def on_update(self):
        """
        This method will be overriden by our strategies.
        """
        pass
class OBIStrategy(Strategy):
    def __init__(self, ticker, data, obi_threshold, initial_cash=100_000, slippage=0.01):
        super().__init__(ticker, initial_cash)
        self.data = data
        self.obi_threshold = obi_threshold
        self.calculate_obi()  # Calculate OBI within constructor
        self.slippage = slippage  # Slippage for market orders

    def calculate_obi(self):
        self.data['OBI'] = (self.data['bid1qty'] - self.data['ask1qty']) / (self.data['bid1qty'] + self.data['ask1qty'])
        self.data['signal'] = 0
        self.data.loc[self.data['OBI'] > self.obi_threshold, 'signal'] = 1
        self.data.loc[self.data['OBI'] < -self.obi_threshold, 'signal'] = -1

    def has_open_position(self, direction):
        for trade in self.positions.values():
            if trade.direction == direction:
                return True
        return False

    def on_update(self):
        obi_signal = self.data.loc[self.current_time, 'signal']

        # Buy condition
        if obi_signal == 1:
            # Close any open sell trades
            if self.has_open_position('S'):
                for trade_id, trade in list(self.open_trades.items()):
                    if trade.direction == 'S':
                        close_order = Order(self.ticker, abs(trade.quantity), 'B', self.current_time, self.slippage)
                        self.order_send(close_order)
                        # You may remove the trade from the dictionary if you want
                        # del self.open_trades[trade_id]

            # Place buy order
            price = self.data.loc[self.current_time, 'ask1px']
            order = Order(self.ticker, 100, 'B', self.current_time, self.slippage)
            order.tp = price + 0.02
            order.sl = price - 0.02
            self.order_send(order)

        # Sell condition
        elif obi_signal == -1:
            # Close any open buy trades
            if self.has_open_position('B'):
                for trade_id, trade in list(self.open_trades.items()):
                    if trade.direction == 'B':
                        close_order = Order(self.ticker, abs(trade.quantity), 'S', self.current_time, self.slippage)
                        self.order_send(close_order)
                        # You may remove the trade from the dictionary if you want
                        # del self.open_trades[trade_id]

            # Place sell order
            price = self.data.loc[self.current_time, 'bid1px']
            order = Order(self.ticker, 100, 'S', self.current_time, self.slippage)
            order.tp = price - 0.02
            order.sl = price + 0.02
            self.order_send(order)

if __name__=="__main__":
    data_processor = DataProcessor('AKBNK.E.csv')
    #creating df's for limit order book and executions
    lob_snaps, execs = data_processor.lob_dfs()
    engine=Engine()
    engine.add_strategy(Strategy())
    engine.add_data(lob_snaps)
    engine.run()

