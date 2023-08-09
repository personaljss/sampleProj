import pandas as pd
import numpy as np


class VWAPCrossoverStrategy:
    def __init__(self, df, stop_loss_pct=0.02):
        self.df = df.copy()  # Copy the dataframe to ensure no in-place modifications
        self.stop_loss_pct = stop_loss_pct
        self.initial_balance = 10000  # assuming starting capital is 100 units
        self.portfolio_value = [self.initial_balance]

    def run(self, small_window, large_window, take_profit_pct):
        self.generate_signals(small_window, large_window, take_profit_pct)
        self.execute_trades(take_profit_pct)
        return self.portfolio_value

    def execute_trades(self, take_profit_pct):
        # Calculate potential exit and stop prices for long and short trades
        self.df['long_exit'] = self.df['execpx'] * (1 + take_profit_pct)
        self.df['short_exit'] = self.df['execpx'] * (1 - take_profit_pct)
        self.df['long_stop'] = self.df['execpx'] * (1 - self.stop_loss_pct)
        self.df['short_stop'] = self.df['execpx'] * (1 + self.stop_loss_pct)

        # Calculate the potential change in price if stop loss is hit
        self.df['potential_price_change'] = self.df['execpx'].diff().abs()

        # Calculate the lot size such that potential loss is 2% of current portfolio
        self.df['lot_size'] = (self.portfolio_value[-1] * self.stop_loss_pct) / self.df['potential_price_change']

        # Calculate profit/loss based on the lot size and price change
        self.df['profit'] = np.where(
            (self.df['signal'] == 1) & ((self.df['execpx'] >= self.df['long_exit']) | (self.df['execpx'] <= self.df['long_stop'])),
            self.df['lot_size'] * self.df['potential_price_change'],
            np.where((self.df['signal'] == -1) & ((self.df['execpx'] <= self.df['short_exit']) | (self.df['execpx'] >= self.df['short_stop'])),
            -self.df['lot_size'] * self.df['potential_price_change'], 0)
        )

        # Calculate the cumulative portfolio value
        self.df['portfolio_value'] = self.df['profit'].cumsum() + self.initial_balance
        self.portfolio_value = self.df['portfolio_value'].tolist()

    def calculate_vwap(self, window):
        vol_sum = self.df['execqty'].rolling(window).sum()
        px_vol_sum = (self.df['execpx'] * self.df['execqty']).rolling(window).sum()
        return px_vol_sum / vol_sum

    def generate_signals(self, small_window, large_window, take_profit_pct):
        self.df['vwap_small'] = self.calculate_vwap(small_window)
        self.df['vwap_large'] = self.calculate_vwap(large_window)

        # 1 for long, -1 for short, 0 for no position
        self.df['signal'] = np.where(self.df['vwap_small'] > self.df['vwap_large'], 1, 
                                     np.where(self.df['vwap_small'] < self.df['vwap_large'], -1, 0))

        
class Trade:
    def __init__(self, entry_price, lot_size, direction, take_profit_pct, stop_loss_pct=0.02):
        self.entry_price = entry_price
        self.exit_price = None
        self.lot_size = lot_size
        self.direction = direction
        self.take_profit_pct = take_profit_pct
        self.stop_loss_pct = stop_loss_pct
        self.is_active = True

    def close(self, exit_price):
        self.exit_price = exit_price
        self.is_active = False

    @property
    def profit(self):
        if self.exit_price is None:
            return 0
        return self.lot_size * (self.exit_price - self.entry_price) * self.direction

