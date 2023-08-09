import pandas as pd
import numpy as np
from strategy import*

def setup_test_data():
    # Sample data: 'execpx' is execution price, 'execqty' is execution quantity.
    data = {
        'execpx': [100, 101, 102, 103, 101, 100, 99, 98, 100, 101],
        'execqty': [10, 10, 10, 10, 10, 10, 10, 10, 10, 10]
    }
    df = pd.DataFrame(data)
    df.index = pd.to_datetime(df.index, unit='s')  # Creating a sample timestamp index for rolling window to work
    return df

def test_signal_generation():
    df = setup_test_data()
    strategy = VWAPCrossoverStrategyOptimized(df, 10)
    strategy.generate_signals(2, 3, 0.03)
    
    expected_signals = [0, 0, 0, 1, -1, -1, 0, 0, 1, 1]  # Adjust based on your sample data and strategy logic
    assert all(strategy.df['signal'] == expected_signals), f"Expected {expected_signals} but got {strategy.df['signal'].tolist()}"

def test_trade_execution():
    df = setup_test_data()
    strategy = VWAPCrossoverStrategyOptimized(df, 10)
    strategy.run(2, 3, 0.03)
    
    # Check if trades are being made. Adjust expected values based on sample data.
    assert len(strategy.trades) == 4, f"Expected 4 trades but got {len(strategy.trades)}"

def test_portfolio_value():
    df = setup_test_data()
    strategy = VWAPCrossoverStrategyOptimized(df, 10)
    strategy.run(2, 3, 0.03)
    
    expected_final_value = 10020  # Adjust based on your sample data and strategy logic
    assert strategy.portfolio_value[-1] == expected_final_value, f"Expected {expected_final_value} but got {strategy.portfolio_value[-1]}"

def run_tests():
    test_signal_generation()
    test_trade_execution()
    test_portfolio_value()
    print("All tests passed!")

run_tests()
