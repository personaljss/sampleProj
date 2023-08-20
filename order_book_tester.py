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

    def __init__(self, ticker, size, side, time, price, order_type='market'):
        self.ticker = ticker
        self.side = side
        self.size = size
        self.type = order_type# this can be market, limit or delete
        self.bist_time = self._adjust_time_with_latency(pd.to_datetime(time))
        self.id = self._generate_id()  # Assign a unique ID to the order when it's created.
        self.price=price
        #tp and sl are not included by the real order in bist however our strategy can use them
        self.sl=0 
        self.tp=np.Infinity
        self.deleted=False # if an order is deleted, this will be True
        self.target_id=0 # this is for deletion orders. It specifies which order should be deleted
        self.exec_data=[] # this list will hold the dicts with keys price, time, volume to handle partial execution
        self.executed_amount : int = 0 #this field can be inferred from exec_data; however, it is not efficient so I preferred to create another field
        self.target_id : str = "" #this field is useful only for delete orders. It is the id of the order to be deleted
    
    @property
    def waiting_volume(self):
        return self.size-self.executed_amount

    @staticmethod
    def _get_latency():
        latency = 0
        while latency <= 0:
            latency = np.random.normal(0.0226, 0.010)
        return pd.Timedelta(seconds=int(latency))
    
    def execute(self, price : float, volume_available : int, time : pd.DatetimeIndex):
        realised_volume=volume_available
        if self.side=="S": realised_volume=-volume_available
        self.exec_data.append({'price':price, 'volume' : realised_volume, 'time':time})
        self.executed_amount+=volume_available

    def _adjust_time_with_latency(self, time):
        return time + self._get_latency()

    @classmethod
    def _generate_id(cls):
        cls._order_id_counter += 1
        return str(cls._order_id_counter)        
    
class Engine():
    
    def __init__(self, data : pd.DataFrame):
        self.strategy : Strategy = None
        self.data : pd.DataFrame = data
        self.current_time = None
        
    def add_data(self, data: pd.DataFrame):
        #checking the validity of the data
        required_columns = {
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
        start_time = time.time()
        if not self.strategy:
            raise ValueError("No strategy has been added. Use the add_strategy method to add one.")
        if self.strategy.data is None or self.strategy.data.empty:
            raise ValueError("No data has been added to the strategy. Use the add_data method to add one.")
        
        initial_balance=self.strategy.cash
        
        for current_time in self.data.index:
            #synchronising the times
            self.current_time = current_time
            self.strategy.current_time = current_time
            # fill orders from previous period
            self._fill_orders()
                    
            # Run the strategy on the current update
            self.strategy.on_update()

        
        last_bid=self.data[self.data['bid1px'] != 0]['bid1px'].iloc[-1]
        last_ask=self.data[self.data['ask1px'] != 0]['ask1px'].iloc[-1]

        end_time = time.time()
        print(f"It took {end_time - start_time} seconds to run the test before result calculations.")
        
        print(f"initial balance: {initial_balance}, final portfolio: {self.strategy.cash} cash and {self.strategy.assets} assets. total:{self.strategy.portfolio_value(current_time)}")
        #return the data frame of executions
        df = pd.DataFrame(self.strategy.executions).set_index('time')        
        return df
    
            
    def _fill_orders(self):
        """
        This method fills buy and sell orders, creating new trade objects and adjusting the strategy's cash balance.
        """
        for order in list(self.strategy.waiting_orders.values()):
            #checking the order time to prevent look ahead. Because there is a latency when sending orders to the market
            if order.bist_time <= self.current_time:
                if order.type=='delete':
                    self.strategy.order_delete(order)
                    continue
                #if it is a sell
                if order.side=='S':
                    offered_price=self.data.loc[self.current_time]['bid1px']
                    #if it is a market order
                    if order.type=='market':
                        volume = min(self.data.loc[self.current_time]['bid1qty'], order.waiting_volume)
                        self.strategy.execute_order(order, offered_price, volume, self.current_time)
                    #if it is a limit order and the offered price is appropriate for execution
                    elif order.type=='limit' and offered_price>=order.price:
                        #the order should be executed by the minimum qty of ask1qty and order.size
                        volume = min(self.data.loc[self.current_time]['bid1qty'], order.waiting_volume)
                        self.strategy.execute_order(order, offered_price, volume, self.current_time)
                #if it is a buy                   
                elif order.side=='B':
                    offered_price=self.data.loc[self.current_time]['ask1px']
                    #if it is a market order    
                    if order.type=='market':
                        volume = min(self.data.loc[self.current_time]['ask1qty'], order.waiting_volume)
                        self.strategy.execute_order(order, offered_price, volume, self.current_time)
                    #if it is a limit order and the offered price is appropriate for execution            
                    elif order.type=='limit' and offered_price>=order.price:
                        #the order should be executed by the minimum qty of ask1qty and order.size
                        volume = min(self.data.loc[self.current_time]['ask1qty'], order.waiting_volume)
                        self.strategy.execute_order(order, offered_price, volume, self.current_time)

class Strategy():
    
    def __init__(self, ticker, initial_cash=0):
        #the symbol name
        self.ticker : str = ticker
        #the current time to keep track of the orderbook 
        self.current_time : pd.DatetimeIndex = None
        self.data : pd.DataFrame = None
        #current cash amount
        self.cash : float = initial_cash
        # this dict will hold the current waiting orders, keys are order id's
        self.waiting_orders : dict = {}
        # this dict will hold all the orders sent by the strategy for further analysis(not important when running tester)
        self.orders : dict = {} 
        #current hold assets
        self.assets : int = 0
        #the current average cost of the hold assets for profit or margin calculations
        self.cost : float = 0
        #the executions indexed by execution time, for furhter analysis
        self.executions : list = []

    def order_send(self,order : Order): 
        self.orders[order.id]=order
        self.waiting_orders[order.id]=order
    
    def order_delete(self, delete_order : Order):
        target_order=self.orders[delete_order.target_id]
        target_order.deleted=True
        del self.waiting_orders[delete_order.id]
        del self.waiting_orders[target_order.id]
    
    def execute_order(self, order : Order,  price : float , volume : int, time : pd.DatetimeIndex):
        #if it is a deletion order, calling the order_delete function and end the current call
        if order.type=='delete':
            self.order_delete(order)
            return
        order.execute(price,volume,time)
        #if the order is fully executed, delete it from the waiting orders
        if order.waiting_volume==0:
            del self.waiting_orders[order.id]
        #adjusting the cash and asset balance
        if order.side=='S':
            volume=-volume
        self.cash+=-volume*price
        self.assets+=volume
        #save the data
        self.executions.append({"time":time, "price" : price, "volume" : volume, "cash": self.cash, "portfolio_value": self.portfolio_value(self.current_time), "assets": self.assets})
    
    
    def portfolio_value(self, time : pd.DatetimeIndex):
        if self.assets>0: offered_price=self.data.loc[time, 'ask1px'] 
        else : offered_price=self.data.loc[time, 'bid1px'] 
        return self.cash+self.assets*offered_price
    
    def on_update(self):
        """
        This method will be overriden by our strategies.
        """
        pass

class OBIStrategy(Strategy):
    """
    This strategy has an entry and exit condition :
    if the imbalance between the best buyer and seller exceeds the entry_threshold and the spread is below the required level, 
    enter the position with 10 percent of the available equity.
    The exit condition is the same logic with another variable called exit_threshold. It also checks the spread.
    all the orders are sent as market orders.
    """
    def __init__(self, ticker,entry_threshold  : float, exit_threshold : float ,initial_cash : float = 1_000_000, spread : float = 0.01):
        super().__init__(ticker,initial_cash)
        self.entry_threshold = entry_threshold
        self.exit_threshold = exit_threshold
        self.spread = spread  # Slippage for market orders
        self.equity_prc=0.1 #this is the part of the equity that will be traded
        if entry_threshold<=exit_threshold:
            raise ValueError("entry_threshold cannot be smaller or equal than exit_threshold")

    def add_data(self, data : pd.DataFrame):
        # copying the data because when we do not do that, there may be warnings
        self.data=data.copy()
        self.calculate_obi() 
    
    def calculate_obi(self):
        self.data['OBI'] = (self.data['bid1qty']-self.data['ask1qty']) / (self.data['bid1qty'] + self.data['ask1qty'])
        # 0 means no trade signal
        self.data['signal'] = 0
        # checking the exit conditions
        # 2 means exit buys
        self.data.loc[self.data['OBI'] <= self.exit_threshold, 'signal'] = 2
        # -2 means exit sells
        self.data.loc[self.data['OBI'] >= -self.exit_threshold, 'signal']=-2
        # 1 means buy
        self.data.loc[self.data['OBI'] >= self.entry_threshold, 'signal'] = 1
        #-1 means sell
        self.data.loc[self.data['OBI'] <= -self.entry_threshold, 'signal'] = -1
        # checking the spread, if it is higher than our treshold, we are not trading
        self.data['spread']=self.data['ask1px'] - self.data['bid1px'] 
        self.data.loc[self.data['spread'] > self.spread, 'signal']=0            

    def on_update(self):
        obi_signal = self.data.loc[self.current_time, 'signal']
        buyers_price = self.data.loc[self.current_time, 'bid1px']
        sellers_price = self.data.loc[self.current_time, 'ask1px']
        equity=self.cash
        #calculating the appropriate lot size(we are entering the positions with minimum of lots that can be bought
        #equity_prc of our equity and 5 percent of available volume)
        if self.assets < 0:
            equity+=self.assets*buyers_price
        available_cash=self.equity_prc*equity
        if available_cash <= 0:
            return        
        # Buy condition
        if obi_signal == 1 and self.cash>0 and self.assets<=0:
            volume=min(self.data.loc[self.current_time,'ask1qty']//20, available_cash//sellers_price)
            # Place buy order
            order = Order(self.ticker, volume, 'B', self.current_time, sellers_price)
            #self.order_send(order)
        # Sell condition
        elif obi_signal == -1  and available_cash>0 and self.assets>=0:
            volume=min(self.data.loc[self.current_time,'bid1qty']//20, available_cash//buyers_price)
            # Place sell order
            order = Order(self.ticker, volume, 'S', self.current_time, sellers_price)
            self.order_send(order)
        #buy exit condition
        elif obi_signal == 2 and self.assets>0:
            order = Order(self.ticker, self.assets, 'S', self.current_time, buyers_price)
            self.order_send(order)
        #sell exit condition
        elif obi_signal == -2 and self.assets<0:
            order = Order(self.ticker, -self.assets, 'B', self.current_time, sellers_price)
            self.order_send(order)

if __name__=="__main__":
    data_processor = DataProcessor('AKBNK.E.csv')
    
    #creating df's for limit order book and executions
    start_time = time.time()
    lob_snaps = data_processor.process()
    end_time = time.time()
    lob_snaps.drop('Mold Package',axis=1).to_csv("my_lob.txt")    
    
    print(f"It took {end_time - start_time} seconds to retrieve the data.")    
    
    data = lob_snaps.between_time('10:00','18:00')
    engine = Engine(data)
    strategy = OBIStrategy("AKBNK",entry_threshold=0.4, exit_threshold=0.2, initial_cash=100_000)
    engine.add_strategy(strategy)

    start_time = time.time()

    result = engine.run()

    end_time = time.time()

    print(f"It took {end_time - start_time} seconds to run the test.")

    print(result.head())

