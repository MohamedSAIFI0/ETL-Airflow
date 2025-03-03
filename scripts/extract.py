import yfinance as yf
import pandas as pd

def extract_data():
    stock_symbols = ["AAPL", "TSLA"]
    data = {}
    
    for symbol in stock_symbols:
        stock = yf.Ticker(symbol)
        hist = stock.history(period="7d")
        data[symbol] = hist
    
    df = pd.concat(data.values(), keys=data.keys()).reset_index()
    df.to_csv(r"C:\Users\P15\Desktop\ETL-Finance\Data\stock_data.csv", index=False)
    print("Data extraction completed.")


extract_data()