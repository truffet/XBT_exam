import os
import pandas as pd
from typing import List
from .fetch_all_trades import fetch_all_trades
from .fetch_new_trades import fetch_new_trades
from .config import symbol, start_time, end_time, count, csv_path


def fetch_xbtusdp_trades_data() -> pd.DataFrame:
    endpoint = "https://www.bitmex.com/api/v1/trade"

    # Set CSV file path and check if it exists
    csv_exists = os.path.exists(csv_path)

    # If the CSV file does not exist, fetch the entire history of data
    if not csv_exists:
        print("Fetching entire history of XBTUSD perpetual contract trades data...")
        trades = fetch_all_trades(endpoint, symbol, start_time, end_time, count)
        df = pd.DataFrame(trades)
        df.to_csv(csv_path, index=False)
        print("Data saved to CSV file:", csv_path)
    # If the CSV file exists, fetch only the new data and update the CSV file
    else:
        print("Checking for new data in XBTUSD perpetual contract trades data...")
        df = pd.read_csv(csv_path, header=None, names=["timestamp", "symbol", "side", "size", "price", "tickDirection", "trdMatchID", "grossValue", "homeNotional", "foreignNotional", "tradeType"])
        last_update_time = df["timestamp"].iloc[-1]
        trades = fetch_new_trades(endpoint, symbol, last_update_time, end_time, count, csv_path)
        if len(trades) > 0:
            new_df = pd.DataFrame(trades)
            updated_df = pd.concat([df, new_df])
            updated_df.to_csv(csv_path, index=False, header=None)
            print("Data updated and saved to CSV file:", csv_path)
        else:
            print("No new data found in XBTUSD perpetual contract trades data.")

    return df

