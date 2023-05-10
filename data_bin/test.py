import pandas as pd

df = pd.read_csv('XBTUSDp_trades.csv')

#check first and last row of XBTUSDp trades data
print("first row:\n")
print(df.head(1))
print("last row:\n")
print(df.tail(1))