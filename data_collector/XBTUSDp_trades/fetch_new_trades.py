import requests
from typing import List
import time
import pandas as pd


def fetch_new_trades(endpoint: str, symbol: str, last_update_time: str, end_time: str, count: int, csv_path: str) -> List[dict]:
    trades = []
    params = {"symbol": symbol, "startTime": last_update_time, "endTime": end_time, "count": count, "reverse": True}
    while True:
        response = requests.get(endpoint, params=params)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch trades data: {response.text}")
        data = response.json()
        trades += data
        if len(data) < count:
            break
        last_update_time = pd.to_datetime(data[-1]["timestamp"])
        params["startTime"] = last_update_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"
        if len(trades) >= count:
            new_df = pd.DataFrame(trades)
            if not pd.read_csv(csv_path).empty:
                new_df.to_csv(csv_path, mode='a', header=False, index=False)
            else:
                new_df.to_csv(csv_path, mode='a', index=False)
            print(f"Fetched and stored {len(trades)} trades ...")
            trades = []
        time.sleep(2)  # Sleep for 2 seconds before making the next request
    return trades
