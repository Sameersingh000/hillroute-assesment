import pandas as pd
import requests
from datetime import datetime

def fetch_ohlcv(symbol, interval, start_date, end_date):
    """
    Fetch OHLCV data for a given symbol and interval using Binance public API.
    Supports incremental fetching for large date ranges.
    """
    # Convert dates to timestamps
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
    end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000)

    # Binance API endpoint for klines
    url = f"https://api.binance.com/api/v3/klines"

    all_data = []
    current_start = start_ts

    while current_start < end_ts:
        # Parameters for the API request
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': current_start,
            'endTime': end_ts,
            'limit': 1000  # Maximum allowed by Binance API
        }

        # Make the API request
        response = requests.get(url, params=params)
        response.raise_for_status()
        klines = response.json()

        if not klines:
            break  # Exit if no more data is returned

        # Append fetched data
        all_data.extend(klines)

        # Update start time for the next batch
        current_start = klines[-1][0] + 1  # Move past the last timestamp

        # Log progress
        print(f"Fetched {len(klines)} rows. Total so far: {len(all_data)} rows.")

    # Create a DataFrame
    df = pd.DataFrame(all_data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ])

    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

    # Select relevant columns
    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

    # Convert numeric columns to float
    numeric_columns = ['open', 'high', 'low', 'close', 'volume']
    df[numeric_columns] = df[numeric_columns].astype(float)

    return df

def verify_data_quality(df):
    """
    Verify the quality of OHLCV data.
    """
    issues = []

    # Check for missing values
    if df.isnull().values.any():
        issues.append("Data contains missing values.")

    # Check for duplicate timestamps
    if df['timestamp'].duplicated().any():
        issues.append("Data contains duplicate timestamps.")

    # Check for non-positive values in numeric columns
    if (df[['open', 'high', 'low', 'close', 'volume']] <= 0).any().any():
        issues.append("Data contains non-positive values in numeric columns.")

    # Prepare quality metrics
    metrics = {
        "total_rows": len(df),
        "missing_values": df.isnull().sum().to_dict(),
        "duplicate_timestamps": df['timestamp'].duplicated().sum(),
        "issues": issues
    }

    return metrics

def calculate_sma(df, column, window):
    """
    Calculate the Simple Moving Average (SMA).
    """
    return df[column].rolling(window=window).mean()

def generate_signals(df):
    """
    Generate buy and sell signals based on SMA conditions.
    """
    df['signal'] = 0
    df.loc[df['SMA_10'] > df['SMA_20'], 'signal'] = 1  # Buy signal
    df.loc[df['SMA_20'] > df['SMA_10'], 'signal'] = -1  # Sell signal
    return df

def simulate_trading(df, initial_balance):
    """
    Simulate trading based on SMA signals and update balance.
    """
    balance = initial_balance
    position = 0  # 0: No position, 1: Long position, -1: Short position
    entry_price = 0  # Track the entry price of the position

    for index, row in df.iterrows():
        if balance < 1000:
            print(f"Insufficient balance (${balance:.2f}). Stopping trades.")
            break

        # Check the last date in the dataset
        is_last_day = row['timestamp'] == df['timestamp'].iloc[-1]

        print(f"Row: {index}, Signal: {row['signal']}, Close: {row['close']}")

        if row['signal'] == 1 and position <= 0:  # Buy condition
            position = 1
            entry_price = row['close']
            print(f"Buying at ${row['close']} on {row['timestamp']}")

        elif row['signal'] == -1 and position >= 0:  # Sell condition
            position = -1
            entry_price = row['close']
            print(f"Selling at ${row['close']} on {row['timestamp']}")

        # Close positions on the last day of trading
        if is_last_day and position != 0:
            if position == 1:  # Closing long position
                balance += (row['close'] - entry_price)
                print(f"Closing long position at ${row['close']} on {row['timestamp']}")
            elif position == -1:  # Closing short position
                balance += (entry_price - row['close'])
                print(f"Closing short position at ${row['close']} on {row['timestamp']}")
            position = 0  # Reset position

    return balance

def calculate_pnl(initial_balance, final_balance):
    """
    Calculate the Profit and Loss (PnL).
    """
    return final_balance - initial_balance

def calculate_cagr(initial_balance, final_balance, years):
    """
    Calculate the Compound Annual Growth Rate (CAGR).
    """
    return ((final_balance / initial_balance) ** (1 / years)) - 1

def main():
    # Specify your parameters here
    symbol = 'BTCUSDT'
    interval = '1d'  # Daily timeframe

    # Example usage of the date function
    start_date = '2021-01-01'  # Start date for SMA calculation
    end_date = '2024-01-01'    # End date for SMA calculation

    # Initial balance
    initial_balance = 10000

    # Fetch OHLCV data
    ohlcv_data = fetch_ohlcv(symbol, interval, start_date, end_date)

    # Verify data quality
    quality_report = verify_data_quality(ohlcv_data)
    print("Data Quality Report:", quality_report)

    # Calculate 10-day and 20-day SMA
    ohlcv_data['SMA_10'] = calculate_sma(ohlcv_data, 'close', 10)
    ohlcv_data['SMA_20'] = calculate_sma(ohlcv_data, 'close', 20)

    # Generate buy and sell signals
    ohlcv_data = generate_signals(ohlcv_data)

    # Simulate trading
    final_balance = simulate_trading(ohlcv_data, initial_balance)
    print(f"Final balance: ${final_balance:.2f}")

    # Calculate PnL and CAGR
    pnl = calculate_pnl(initial_balance, final_balance)
    print(f"PnL: ${pnl:.2f}")

    # Calculate CAGR (in years)
    years = (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days / 365.0
    cagr = calculate_cagr(initial_balance, final_balance, years)
    print(f"CAGR: {cagr * 100:.2f}%")

    # Save to CSV if needed
    ohlcv_data.to_csv(f"{symbol}_SMA_Signals_{start_date}_to_{end_date}.csv", index=False)

if __name__ == "__main__":
    main()
