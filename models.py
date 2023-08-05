from sortedcontainers import SortedDict
import logging
import pandas as pd

class OrderBook:
    """
    The OrderBook class represents a market order book, where orders from traders are stored.
    Each order book is separated into two parts: bids (buy orders) and asks (sell orders).
    The SortedDict structure is used to have the prices sorted, which is necessary for the operation of an order book.
    """

    def __init__(self):
        """
        Initialize the order book. Bids and asks are represented with SortedDict for efficient access and order.
        Two additional dictionaries are maintained to store quantity associated with each price point.
        Each order is identified by its unique 'order_id' and can be quickly accessed using these dictionaries.
        """
        self.bids = SortedDict()  # Sorted dictionary for bids
        self.asks = SortedDict()  # Sorted dictionary for asks
        self.bid_order_dict = {}  # Dictionary for bid order objects with their order_ids
        self.ask_order_dict = {}  # Dictionary for ask order objects with their order_ids
        self.bid_qty_dict = {}    # Dictionary for quantities associated with each bid price
        self.ask_qty_dict = {}    # Dictionary for quantities associated with each ask price

    def add_order(self, order):
        """
        Method to add an order to the order book.
        The type of the order is checked, and then depending on whether it's a bid or ask, the order is added to the correct dictionary.
        The quantity dictionary is also updated with the new order's quantity.
        """
        if order.msg_type == 'A':  # If it's an 'add order' message
            if order.side == 'B':  # If it's a bid order
               # If this price point doesn't exist yet in the bid order dictionary, initialize it with an empty list and zero quantity
                if order.price not in self.bids:
                    self.bids[order.price] = []
                    self.bid_qty_dict[order.price] = 0
                # Add the new bid order to the bid order list and update the quantity dictionary
                self.bids[order.price].append(order)
                self.bid_order_dict[order.order_id] = order
                self.bid_qty_dict[order.price] += order.qty
            else:  # 'S' If it's an ask order
                # Similar logic as the bid order, but for the ask order
                if order.price not in self.asks:
                    self.asks[order.price] = []
                    self.ask_qty_dict[order.price] = 0
                self.asks[order.price].append(order)
                self.ask_order_dict[order.order_id] = order
                self.ask_qty_dict[order.price] += order.qty
        elif order.msg_type == 'D':  # If it's a 'delete order' message
            self.delete_order(order)
        elif order.msg_type == 'E':  # If it's an 'execute order' message
            self.execute_order(order)

    def delete_order(self, order):
        """
        This method deletes an order from the order book.
        It first checks whether it's a bid or ask order and then removes the order from the correct dictionary.
        If the order to be deleted is not found in the dictionary, it raises an error.
        """
        if order.side == 'B':  # If it's a bid order
            if order.order_id in self.bid_order_dict:  # If the bid order is in the dictionary
                # Get the original order from the dictionary
                original_order = self.bid_order_dict[order.order_id]
                if original_order.price in self.bids:
                    # Remove the order from the bids dictionary and update the quantity dictionary
                    self.bids[original_order.price].remove(original_order)
                    self.bid_qty_dict[original_order.price] -= original_order.qty
                    # If no more orders are associated with this price, remove it from the dictionary
                    if not self.bids[original_order.price]:
                        del self.bids[original_order.price]
                        del self.bid_qty_dict[original_order.price]
                # Remove the order from the bid_order_dict
                del self.bid_order_dict[order.order_id]
            else:  # If the order doesn't exist, print an error
                logging.error(f"Bid order with ID {order.order_id} does not exist.")
        else:  # 'S', If it's an ask order, the process is similar to the bid order
            if order.order_id in self.ask_order_dict:
                original_order = self.ask_order_dict[order.order_id]
                if original_order.price in self.asks:
                    self.asks[original_order.price].remove(original_order)
                    self.ask_qty_dict[original_order.price] -= original_order.qty
                    if not self.asks[original_order.price]:
                        del self.asks[original_order.price]
                        del self.ask_qty_dict[original_order.price]
                del self.ask_order_dict[order.order_id]
            else:
                logging.error(f"Ask order with ID {order.order_id} does not exist.")

    def execute_order(self, order):
        """
        This method executes an order in the order book.
        It first checks whether it's a bid or ask order, finds the existing order in the correct dictionary, and then reduces the quantity of the order.
        If the order quantity becomes zero or less, the order is deleted from the book.
        """
        if order.side == 'B':  # If it's a bid order
            if order.order_id in self.bid_order_dict:  # If the bid order is in the dictionary
                existing_order = self.bid_order_dict[order.order_id]  # Get the existing order
                existing_order.qty -= order.qty  # Decrease the quantity of the existing order
                self.bid_qty_dict[existing_order.price] -= order.qty  # Update the quantity dictionary
                if existing_order.qty <= 0:  # If the quantity is zero or less, delete the order
                    self.delete_order(existing_order)
            else:  # If the order doesn't exist, print an error
                logging.error(f"Bid order with ID {order.order_id} does not exist.")
        else:  # 'S', If it's an ask order, the process is similar to the bid order
            if order.order_id in self.ask_order_dict:
                existing_order = self.ask_order_dict[order.order_id]
                existing_order.qty -= order.qty
                self.ask_qty_dict[existing_order.price] -= order.qty
                if existing_order.qty <= 0:
                    self.delete_order(existing_order)
            else:
                logging.error(f"Ask order with ID {order.order_id} does not exist.")
    def snapshot(self, timestamp, asset_name):
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
            f"A-{side}-{price}-{order.qty}-{order.order_id}"
            for side, book in [('B', self.bids), ('S', self.asks)]
            for price in (bid_prices if side == 'B' else ask_prices)
            for order in book.get(price, [])
        ]

        mold_package = ";".join(mold_packages)

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
        }

# End of OrderBook class definition

class Order:
    """
    The Order class represents an order in the order book.
    Each order has a network_time, bist_time, msg_type (A for Add, D for Delete, E for Execute), asset_name, side (B for Bid, S for Ask), price, que_loc, qty (quantity), and order_id.
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

def process_orders(df):
    """
    This function processes a DataFrame of orders.
    It creates an OrderBook object, and for each order in the DataFrame, it adds the order to the order book and creates a snapshot of the order book state.
    It returns the final state of the order book and a DataFrame of all the snapshots.
    """
    order_book = OrderBook()
    snapshots = []

    df_records = df.to_dict('records')

    for record in df_records:
        order = Order(**record)  # Create an Order object from the record
        order_book.add_order(order)  # Add the order to the order book
        snapshot = order_book.snapshot(order.network_time, order.asset_name)  # Create a snapshot of the order book
        if len(snapshots)>0 and snapshots[-1]['Date']==snapshot['Date']:  # If the last snapshot has the same timestamp as the current snapshot
            snapshots[-1]=snapshot  # Replace the last snapshot with the current snapshot
        else:
            snapshots.append(snapshot)  # Otherwise, append the current snapshot to the list of snapshots

    snapshot_df = pd.DataFrame(snapshots)  # Convert the list of snapshots to a DataFrame
    return order_book, snapshot_df  # Return the final state of the order book and the DataFrame of snapshots
