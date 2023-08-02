from sortedcontainers import SortedDict
import logging
import pandas as pd

class OrderBook:
    def __init__(self):
        self.bids = SortedDict()
        self.asks = SortedDict()
        self.bid_order_dict = {}
        self.ask_order_dict = {}
        self.bid_qty_dict = {}
        self.ask_qty_dict = {}

    def add_order(self, order):
        if order.msg_type == 'A':
            if order.side == 'B':
                if order.price not in self.bids:
                    self.bids[order.price] = []
                    self.bid_qty_dict[order.price] = 0
                self.bids[order.price].append(order)
                self.bid_order_dict[order.order_id] = order
                self.bid_qty_dict[order.price] += order.qty
            else:  # 'S'
                if order.price not in self.asks:
                    self.asks[order.price] = []
                    self.ask_qty_dict[order.price] = 0
                self.asks[order.price].append(order)
                self.ask_order_dict[order.order_id] = order
                self.ask_qty_dict[order.price] += order.qty
        elif order.msg_type == 'D':
            self.delete_order(order)
        elif order.msg_type == 'E':
            self.execute_order(order)

    def delete_order(self, order):
        if order.side == 'B':
            if order.order_id in self.bid_order_dict:
                original_order = self.bid_order_dict[order.order_id]
                if original_order.price in self.bids:
                    self.bids[original_order.price].remove(original_order)
                    self.bid_qty_dict[original_order.price] -= original_order.qty
                    if not self.bids[original_order.price]:
                        del self.bids[original_order.price]
                        del self.bid_qty_dict[original_order.price]
                del self.bid_order_dict[order.order_id]
            else:
                logging.error(f"Bid order with ID {order.order_id} does not exist.")
        else:  # 'A'
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
        if order.side == 'B':
            if order.order_id in self.bid_order_dict:
                existing_order = self.bid_order_dict[order.order_id]
                existing_order.qty -= order.qty
                self.bid_qty_dict[existing_order.price] -= order.qty
                if existing_order.qty <= 0:
                    self.delete_order(existing_order)
            else:
                logging.error(f"Bid order with ID {order.order_id} does not exist.")
        else:  # 'A'
            if order.order_id in self.ask_order_dict:
                existing_order = self.ask_order_dict[order.order_id]
                existing_order.qty -= order.qty
                self.ask_qty_dict[existing_order.price] -= order.qty
                if existing_order.qty <= 0:
                    self.delete_order(existing_order)
            else:
                logging.error(f"Ask order with ID {order.order_id} does not exist.")

    def snapshot(self, timestamp, asset_name):
        bid_prices = list(self.bids.keys())[-3:]  # get top 3 bid prices
        bid_prices.reverse()  # reverse to get highest price first
        ask_prices = list(self.asks.keys())[:3]  # get top 3 ask prices

        bid_qtys = [self.bid_qty_dict.get(price, 0) for price in bid_prices]
        ask_qtys = [self.ask_qty_dict.get(price, 0) for price in ask_prices]

        mold_packages = [
            f"A-{side}-{price}-{order.qty}-{order.order_id}"
            for side, book in [('B', self.bids), ('S', self.asks)]
            for price in (bid_prices if side == 'B' else ask_prices)
            for order in book.get(price, [])
        ]

        mold_package = ";".join(mold_packages)

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

class Order:
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
    order_book = OrderBook()
    snapshots = []

    df_records = df.to_dict('records')

    for record in df_records:
        order = Order(**record)
        order_book.add_order(order)
        snapshot = order_book.snapshot(order.network_time, order.asset_name)
        if len(snapshots)>0 and snapshots[-1]['Date']==snapshot['Date']:
            snapshots[-1]=snapshot
        else:
            snapshots.append(snapshot)

    snapshot_df = pd.DataFrame(snapshots)
    return order_book, snapshot_df
