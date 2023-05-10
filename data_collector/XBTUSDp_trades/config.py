import pandas as pd

symbol = "XBTUSD"
start_time = None  # Start from the earliest data available
end_time = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")  # End at the current time
count = 1000  # Number of trades to fetch in each request
csv_path = "../data_bin/XBTUSDp_trades.csv"
