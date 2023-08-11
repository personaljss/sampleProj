from sortedcontainers import SortedDict
import logging
import pandas as pd
import tqdm

# Adding to models.py
class DataProcessor:
    """
    DataProcesser class is responsible for loading and pre-processing the trading data.
    """
    
    def __init__(self, file_path='AKBNK.E.csv'):
        self.data = self.load_data(file_path)

    def load_data(self,filename) -> pd.DataFrame:
        """
        Load the trading data from the specified file and preprocess it.
        :param filename: Name of the file containing the trading data.
        :return: Processed DataFrame containing the trading data.
        """
        # Using pandas' read_csv function to read the data.
        data = pd.read_csv(filename)

        # Adding column names to the data frame
        column_names = ["network_time", "bist_time", "msg_type", "asset_name", "side", "price", "que_loc", "qty", "order_id"]
        data.columns = column_names

        # Removing unnecessary orders from the data frame (only the add, execute, and delete orders should be there)
        data = data[data["msg_type"].isin(["A", "E", "D"])]
        
        # Resetting the indexes to start from 0
        data.reset_index(drop=True, inplace=True)

        # Converting unix time to date-time objects
        data['network_time'] = (pd.to_datetime(data['network_time'], unit='ns')
                                .dt.tz_localize('UTC')
                                .dt.tz_convert('Europe/Istanbul'))

            
        data['bist_time'] = pd.to_datetime(data['bist_time'], unit='ns').dt.tz_localize('UTC').dt.tz_convert('Europe/Istanbul')

        return data
    
    def process(self) -> pd.DataFrame:
        """
        This function processes a DataFrame of orders.
        It creates an OrderBook object, and for each order in the DataFrame, 
        it adds the order to the order book and creates a snapshot of the order book state.
        It returns a DataFrame of all the snapshots.
        """
        order_book = OrderBook()
        snapshots = []

        df_records = self.data.to_dict('records')

        for record in df_records:
            message = Message(**record)  # Create an Order object from the record
            order_book.on_new_message(message)  # Add the order to the order book
            snapshot = order_book.snapshot(message.network_time, message.asset_name)  # Create a snapshot of the order book
            if len(snapshots)>0 and snapshots[-1]['Date']==snapshot['Date']:  # If the last snapshot has the same timestamp as the current snapshot
                snapshots[-1]=snapshot  # Replace the last snapshot with the current snapshot
            else:
                snapshots.append(snapshot)  # Otherwise, append the current snapshot to the list of snapshots

        snapshot_df = pd.DataFrame(snapshots)  # Convert the list of snapshots to a DataFrame
        snapshot_df.set_index('Date',inplace=True)
        return snapshot_df        

    

class OrderBook:
    """
    The OrderBook class represents a market order book, where orders from traders are stored.
    """

    def __init__(self):
        """
        Initialize the order book.
        """
        self.bids = SortedDict()
        self.asks = SortedDict()
        self.bid_order_dict = {}
        self.ask_order_dict = {}
        self.bid_qty_dict = {}
        self.ask_qty_dict = {}
        self.executions = {}  # A dictionary to represent executions

    def on_new_message(self, message):
        """
        Method to handle different types of messages and modify the order book accordingly.
        """
        if message.msg_type == 'A':
            self.add_order(message)
        elif message.msg_type == 'D':
            self.delete_order(message)
        elif message.msg_type == 'E':
            self.execute_order(message)

    def add_order(self, message):
        """
        Adds an order to the book based on its type.
        """
        side_book = self.bids if message.side == 'B' else self.asks
        order_dict = self.bid_order_dict if message.side == 'B' else self.ask_order_dict
        qty_dict = self.bid_qty_dict if message.side == 'B' else self.ask_qty_dict

        if message.price not in side_book:
            side_book[message.price] = []
            qty_dict[message.price] = 0

        side_book[message.price].append(message)
        order_dict[message.order_id] = message
        qty_dict[message.price] += message.qty

    def delete_order(self, message):
        """
        Deletes an order from the book.
        """
        side_book = self.bids if message.side == 'B' else self.asks
        order_dict = self.bid_order_dict if message.side == 'B' else self.ask_order_dict
        qty_dict = self.bid_qty_dict if message.side == 'B' else self.ask_qty_dict

        if message.order_id in order_dict:
            order = order_dict[message.order_id]
            side_book[order.price].remove(order)
            qty_dict[order.price] -= order.qty

            if not side_book[order.price]:
                del side_book[order.price]
                del qty_dict[order.price]

            del order_dict[message.order_id]
        else:
            logging.error(f"Order with ID {message.order_id} does not exist.")

    def execute_order(self, message):
        """
        Executes or updates an order in the book.
        """
        order_dict = self.bid_order_dict if message.side == 'B' else self.ask_order_dict
        qty_dict = self.bid_qty_dict if message.side == 'B' else self.ask_qty_dict

        if message.order_id in order_dict:
            order = order_dict[message.order_id]
            order.qty -= message.qty
            qty_dict[order.price] -= message.qty
            # Log the execution, assuming only one execution can happen at a single network time,
            self.executions[message.network_time] = {
                'execpx': order.price,
                'execqty': message.qty
            }
            # If fully executed, remove from book
            if order.qty <= 0:
                self.delete_order(message)

    
    def snapshot(self, timestamp, asset_name) -> dict:
        """
        This method creates a snapshot of the current state of the order book.
        It takes the top 3 bid and ask prices and quantities from the respective dictionaries.
        The snapshot also includes a 'Mold Package', which is a string representation of all the orders in the order book.
        """
        # Extract top 3 bid and ask prices
        bid_prices = list(self.bids.keys())[-3:]  # get top 3 bid prices
        bid_prices.reverse()  # reverse to get highest price first
        ask_prices = list(self.asks.keys())[:3]  # get top 3 ask prices

        # Extract quantities for top 3 bid and ask prices
        bid_qtys = [self.bid_qty_dict.get(price, 0) for price in bid_prices]
        ask_qtys = [self.ask_qty_dict.get(price, 0) for price in ask_prices]

        # Generate the 'Mold Package', which is a string representation of all the orders
        mold_packages = [
            f"{order.msg_type}-{side}-{price}-{order.qty}-{order.order_id}"
            for side, book in [('B', self.bids), ('S', self.asks)]
            for price in (bid_prices if side == 'B' else ask_prices)
            for order in book.get(price, [])
        ]

        mold_package = ";".join(mold_packages)

        #get the execution data, if no execution happened in this update, execpx and execqty are 0
        exc_dict=self.executions.get(timestamp,{'execpx' : 0, 'execqty' : 0})


        # Return a dictionary representing the snapshot
        return {
            "Date": timestamp,  # use the passed timestamp
            "Asset": asset_name,
            "bid1qty": bid_qtys[0] if bid_qtys else 0,
            "bid1px": bid_prices[0] if bid_prices else 0,
            "bid2qty": bid_qtys[1] if len(bid_qtys) > 1 else 0,
            "bid2px": bid_prices[1] if len(bid_prices) > 1 else 0,
            "bid3qty": bid_qtys[2] if len(bid_qtys) > 2 else 0,
            "bid3px": bid_prices[2] if len(bid_prices) > 2 else 0,
            "ask1px": ask_prices[0] if ask_prices else 0,
            "ask1qty": ask_qtys[0] if ask_qtys else 0,
            "ask2px": ask_prices[1] if len(ask_prices) > 1 else 0,
            "ask2qty": ask_qtys[1] if len(ask_qtys) > 1 else 0,
            "ask3px": ask_prices[2] if len(ask_prices) > 2 else 0,
            "ask3qty": ask_qtys[2] if len(ask_qtys) > 2 else 0,
            "Mold Package": mold_package,
            "execpx" : exc_dict['execpx'],
            "execqty" : exc_dict['execqty']
        }
    

class Message:
    """
    Represents a message about an order, its execution or its deletion.
    """

    def __init__(self, network_time, bist_time, msg_type, asset_name, side, price, que_loc, qty, order_id):
        self.network_time = network_time
        self.bist_time = bist_time
        self.msg_type = msg_type
        self.asset_name = asset_name
        self.side = side
        self.price = price
        self.que_loc = que_loc
        self.qty = qty
        self.order_id = order_id

