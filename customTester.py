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
    def __init__(self, ticker,side,size,price,time,sl,tp):
        self.ticker = ticker
        self.side = side
        self.price = price
        self.size = size
        self.time = time
        self.sl = sl
        self.tp = tp
        self.id = self._generate_id()  # Assign a unique ID to the order when it's created.
    @classmethod
    def _generate_id(cls):
        cls._order_id_counter += 1
        return str(cls._order_id_counter)
    
class Engine():
    def __init__(self,):
        self.strategy = None
        self.data = None
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
        
        self.strategy.data = self.data

        # We need to preprocess a few things before running the backtest
        self.strategy.data = self.data
        
        for time in tqdm(self.data.index):
            #synchronising the times
            self.current_time = time
            self.strategy.current_time = time
            # fill orders from previus period
            self._fill_orders()
            
            # Run the strategy on the current bupdate
            self.strategy.on_update()
            
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
            

class Strategy():
    def __init__(self, ticker, initial_cash=100_000):
        self.current_time = None
        self.data = None
        self.cash = initial_cash
        self.orders = {}
        self.opened_trades = {}
        self.closed_trades = {}
        self.ticker=ticker

    def order_send(self,order):
        self.orders[order.id]=order
    
    def order_delete(self, order):
        del self.order_delete[order.id]
    
    def position_open(self,order,price, volume, time):
        #create a new trade, add it to the open trades
        trade = Trade(self.ticker,order.side, volume, price, time,order.sl, order.tp)
        self.opened_trades[trade.id] = trade
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
        

    def position_close(self, trade, price, volume, time):
        #add the trade to closed trades, delete it from the open trades if it is fully executed. 
        # if not, deduct the volume from order size. Fİnaly adjust the cash balance
        self.closed_trades[trade.id]=trade
        pass
    
    @property
    def position_size(self):
        return sum([t.size for t in self.trades])
        
    def on_update(self):
        """This method will be overriden by our strategies.
        """
        pass


if __name__=="__main__":
    data_processor = DataProcessor('AKBNK.E.csv')
    #creating df's for linit order book and executions
    lob_snaps, execs = data_processor.lob_dfs()
    engine=Engine()
    engine.add_strategy(Strategy())
    engine.add_data(execs)
    engine.run()

