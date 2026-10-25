import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from datetime import datetime
import re
import csv


DB_URL = 'postgresql://freshw:freshw@localhost:5434/freshcart_dw'
engine = create_engine(DB_URL)

BASE_DIR = './source_files/suppliers'
FRESH_DIR = os.path.join(BASE_DIR, 'fresh')
GENERAL_DIR = os.path.join(BASE_DIR, 'general')

def load_data_with_idempotency(df, table_name, filename):
    
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze_suppliers;"))
    
    inspector = inspect(engine)
    if inspector.has_table(table_name, schema='bronze_suppliers'):
        existing_columns = [col['name'] for col in inspector.get_columns(table_name, schema='bronze_suppliers')]
        
        with engine.begin() as conn:
            for col in df.columns:
                if col not in existing_columns:
                    print(f" Alert: A new column '{col}' was detected in file {filename}. Adding it to the table...")
                    conn.execute(text(f'ALTER TABLE bronze_suppliers.{table_name} ADD COLUMN "{col}" TEXT;'))
            
            delete_query = text(f"DELETE FROM bronze_suppliers.{table_name} WHERE _file_name = :fname")
            conn.execute(delete_query, {"fname": filename})
            
    df.to_sql(table_name, engine, schema='bronze_suppliers', if_exists='append', index=False)

def process_fresh_supplier():
    print("Starting Fresh supplier ingestion...")
    if not os.path.exists(FRESH_DIR):
        return
        
    for filename in os.listdir(FRESH_DIR):
        if filename.endswith('.csv'):
            filepath = os.path.join(FRESH_DIR, filename)
            
            match = re.search(r'fresh_stock_(\d{4}-\d{2}-\d{2})\.csv', filename)
            business_date = match.group(1) if match else None
            
            df = pd.read_csv(filepath)
            
            df.columns = df.columns.str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.lower()
            
            df['_file_name'] = filename
            df['_business_date'] = pd.to_datetime(business_date).date() if business_date else None
            df['_loaded_at'] = pd.Timestamp.now()
            
            load_data_with_idempotency(df, 'fresh_stock', filename)
            print(f"Loaded successfully: {filename}")

def process_general_supplier():
    print("Starting General supplier ingestion...")
    if not os.path.exists(GENERAL_DIR):
        return
        
    for filename in os.listdir(GENERAL_DIR):
        if filename.endswith('.csv'):
            filepath = os.path.join(GENERAL_DIR, filename)
            
            match = re.search(r'GENERAL_STOCK_(\d{8})\.csv', filename)
            business_date = datetime.strptime(match.group(1), '%Y%m%d').strftime('%Y-%m-%d') if match else None
            
            with open(filepath, 'r', encoding='utf-8') as f:
                first_line = f.readline()
                try:
                    detected_sep = csv.Sniffer().sniff(first_line).delimiter
                except:
                    detected_sep = ','
            
            df = pd.read_csv(filepath, sep=detected_sep)
            
            df.columns = df.columns.str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.lower()
            
            df['_file_name'] = filename
            df['_business_date'] = pd.to_datetime(business_date).date() if business_date else None
            df['_loaded_at'] = pd.Timestamp.now()
            
            load_data_with_idempotency(df, 'general_stock', filename)
            print(f"Loaded successfully: {filename} (Delimiter: '{detected_sep}')")

if __name__ == '__main__':
    process_fresh_supplier()
    print("-" * 30)
    process_general_supplier()
    print("All supplier files ingested successfully!")