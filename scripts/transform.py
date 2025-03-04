import pandas as pd

def transform_data():
    df = pd.read_csv(r"C:\Users\P15\Desktop\ETL-Finance\Data\stock_data.csv")
    

    df.dropna(inplace=True)
    df['Date'] = pd.to_datetime(df['Date'])
    df.rename(columns={'Close': 'Closing_Price'}, inplace=True)

    df.to_csv(r"C:\Users\P15\Desktop\ETL-Finance\Data\stock_data_cleaned.csv", index=False)
    print("Data transformation completed.")

