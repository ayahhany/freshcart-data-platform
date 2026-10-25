import os
import time
import requests
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from datetime import datetime, timedelta
from dotenv import load_dotenv


load_dotenv()

API_KEY = os.getenv("FX_API_KEY")
HEADERS = {"X-API-Key": API_KEY}
BASE_URL = "http://localhost:8000/v1/rates/history"

DB_URL = 'postgresql://freshw:freshw@localhost:5434/freshcart_dw'
engine = create_engine(DB_URL)

def fetch_fx_history(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    current_start = start_date
    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=30), end_date)
        s_str = current_start.strftime("%Y-%m-%d")
        e_str = current_end.strftime("%Y-%m-%d")
        
        url = f"{BASE_URL}?start_date={s_str}&end_date={e_str}"
        print(f"Fetching FX history from {s_str} to {e_str}...")
        
        try:
            response = requests.get(url, headers=HEADERS, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                records = []
                
                days_list = data.get('days', [])
                for day_item in days_list:
                    rate_date = day_item.get('rate_date')
                    rates_dict = day_item.get('rates', {})
                    
                    for currency, rate in rates_dict.items():
                        records.append({
                            'date': rate_date,
                            'currency': currency,
                            'rate_to_usd': rate
                        })
                
                if records:
                    df = pd.DataFrame(records)
                    df['_loaded_at'] = pd.Timestamp.now()
                    
                    with engine.begin() as conn:
                        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze_fx;"))
                    
                    
                    inspector = inspect(engine)
                    if inspector.has_table('rates', schema='bronze_fx'):
                        with engine.begin() as conn:
                            conn.execute(text("DELETE FROM bronze_fx.rates WHERE date BETWEEN :s AND :e"), {"s": s_str, "e": e_str})
                    
                    
                    df.to_sql('rates', engine, schema='bronze_fx', if_exists='append', index=False)
                        
                    print(f"Successfully fetched and saved {len(records)} FX rates from {s_str} to {e_str}")
                else:
                    print(f" Warning: No rates found for range {s_str} to {e_str}")
            else:
                print(f" Failed with status code {response.status_code}: {response.text}")
                
        except Exception as e:
            print(f" Error: {e}")
            
        current_start = current_end + timedelta(days=1)

if __name__ == '__main__':
    start_history_date = "2026-10-13"
    today_date = pd.Timestamp.now().strftime('%Y-%m-%d')
    
    print("Starting FX History Initial Full Load...")
    fetch_fx_history(start_history_date, today_date)
    print("FX History ingestion completed!")