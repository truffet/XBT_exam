import requests
from typing import List
import time
import pandas as pd

def fetch_all_trades(endpoint: str, symbol: str, start_time: str, end_time: str, count: int, csv_path: str) -> List[dict]:
    trades = []
    params = {"symbol": symbol, "reverse": True, "count": count}
    while True:
        response = requests.get(endpoint, params=params)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch trades data: {response.text}")
        data = response.json()
        trades += data
        if len(data) < count:
            break
        end_time = pd.to_datetime(data[-1]["timestamp"])
        params["endTime"] = end_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"
        time.sleep(2)  # Sleep for 2 seconds before making the next request
        df = pd.DataFrame(trades)
        df.to_csv(csv_path, index=False, mode="a", header=False)
        trades = []
        print(f"Fetched and stored {len(df)} trades ...")
    return df
