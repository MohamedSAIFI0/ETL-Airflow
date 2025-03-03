import mysql.connector
import pandas as pd

def load_data():
    # Load transformed data
    df = pd.read_csv(r"C:\Users\P15\Desktop\ETL-Finance\Data\stock_data_cleaned.csv")

    # Connect to MySQL
    conn = mysql.connector.connect(
        host="localhost",
        user="root",  # Change to your MySQL username
        password="",  # Change to your MySQL password
        database="stock_db"
    )
    cur = conn.cursor()

    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stocks (
        id INT AUTO_INCREMENT PRIMARY KEY,
        stock_symbol VARCHAR(10),
        date DATE,
        closing_price FLOAT
    )
    """
    cur.execute(create_table_query)

    # Insert data
    insert_query = """
    INSERT INTO stocks (stock_symbol, date, closing_price) 
    VALUES (%s, %s, %s)
    """
    for _, row in df.iterrows():
        cur.execute(insert_query, (row['stock_symbol'], row['date'], row['closing_price']))

    conn.commit()
    cur.close()
    conn.close()
    print("Data successfully loaded into MySQL.")

if __name__ == "__main__":
    load_data()
