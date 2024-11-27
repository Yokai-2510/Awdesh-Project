import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests
import time 

def fetch_historical_data(instrument_key, token):
    india_tz = ZoneInfo("Asia/Kolkata")
    current_date = datetime.now(india_tz)
    from_date = (current_date - timedelta(days=10)).strftime('%Y-%m-%d')
    to_date = current_date.strftime('%Y-%m-%d')
    
    url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/1minute/{to_date}/{from_date}"
    headers = {'accept': 'application/json', 'Api-Version': '2.0', 'Authorization': f'Bearer {token}'}
    
    try:
        response = requests.get(url, headers=headers).json()
        candles_data = response.get('data', {}).get('candles', [])
        
        # Process historical data directly
        processed_rows = []
        for row in candles_data:
            dt = pd.to_datetime(row[0], errors='coerce')  # First element is datetime
            if pd.isna(dt):  # If parsing failed, skip this row
                continue
            processed_rows.append([dt.date(), dt.strftime('%H:%M'), float(row[1]), float(row[2]), float(row[3]), 
                                   float(row[4]), float(row[5] if len(row) > 5 else 0), float(row[6] if len(row) > 6 else 0)])
        
        df = pd.DataFrame(processed_rows, columns=['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest'])

        # Standardize DataFrame directly here
        if df.empty:
            return pd.DataFrame(columns=['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest'])
        
        # Standardize columns and fill missing data
        standard_columns = ['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest']
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        df['Time'] = pd.to_datetime(df['Time'], errors='coerce').dt.strftime('%H:%M')
        
        numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest']
        for col in numeric_columns:
            if col not in df.columns:
                df[col] = 0.0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        return df[standard_columns]
    
    except Exception as e:
        print(f"Error fetching historical data: {e}")
        return pd.DataFrame()

def fetch_intraday_data(instrument_key, token):
    url = f"https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/1minute"
    headers = {'accept': 'application/json', 'Api-Version': '2.0', 'Authorization': f'Bearer {token}'}
    
    try:
        response = requests.get(url, headers=headers).json()
        candles_data = response.get('data', {}).get('candles', [])
        
        # Process intraday data directly
        processed_rows = []
        for row in candles_data:
            dt = pd.to_datetime(row[0], errors='coerce')  # First element is datetime
            if pd.isna(dt):  # If parsing failed, skip this row
                continue
            processed_rows.append([dt.date(), dt.strftime('%H:%M'), float(row[1]), float(row[2]), float(row[3]), 
                                   float(row[4]), float(row[5] if len(row) > 5 else 0), float(row[6] if len(row) > 6 else 0)])
        
        df = pd.DataFrame(processed_rows, columns=['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest'])

        # Standardize DataFrame directly here
        if df.empty:
            return pd.DataFrame(columns=['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest'])

        # Standardize columns and fill missing data
        standard_columns = ['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest']
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        df['Time'] = pd.to_datetime(df['Time'], errors='coerce').dt.strftime('%H:%M')
        
        numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest']
        for col in numeric_columns:
            if col not in df.columns:
                df[col] = 0.0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        return df[standard_columns]
    
    except Exception as e:
        print(f"Error fetching intraday data: {e}")
        return pd.DataFrame()


def fetch_websocket_data(market_data):
    # Convert to DataFrame from 'websocket_candle_data' in market_data
    df = market_data.get('websocket_candle_data')
    
    # Create a copy of the DataFrame to avoid modifying the original data
    df = df.copy()  
    
    # If the columns 'Volume' and 'OpenInterest' don't exist, they will be created with default values
    if 'Volume' not in df.columns:
        df['Volume'] = 0.0
    if 'OpenInterest' not in df.columns:
        df['OpenInterest'] = 0.0
    
    # Ensure time format consistency:
    # Replace any space-separated time with colon-separated format
    df['Time'] = df['Time'].apply(lambda x: x.replace(" ", ":") if isinstance(x, str) else x)
    
    # Convert to datetime format, ensure format is %H:%M:%S and handle errors properly
    # Coerce errors to NaT and then format the datetime object to '%H:%M'
    df['Time'] = pd.to_datetime(df['Time'], format='%H:%M:%S', errors='coerce')
    df['Time'] = df['Time'].dt.strftime('%H:%M')  # Extract only hours and minutes
    
    # Ensure the DataFrame has the required column names and order
    df = df[['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest']]
    
    return df


def fetch_candle_data(market_data):
    instrument_key = "NSE_INDEX|Nifty 50"
    token = "your_access_token"
    candles_count = 5
    
    while True: 
        time.sleep(2)  # Wait for 2 seconds before fetching data again
        try:
            
            # Fetch historical data in every iteration
            historical_df = fetch_historical_data(instrument_key, token)  
            market_data['historical_candle_data'] = historical_df  # Update historical candle data
            
            # Fetch intraday data
            intraday_df = fetch_intraday_data(instrument_key, token)  
            market_data['intraday_candle_data'] = intraday_df  # Update intraday candle data

            # Fetch websocket data by passing market_data
            websocket_df = fetch_websocket_data(market_data)  # Fetch websocket data and format it
            
            # Combine historical, intraday, and websocket data
            dfs = [df for df in [historical_df, intraday_df, websocket_df] if not df.empty]
            if not dfs:
                continue

            # Combine all the data sources and clean up
            combined_df = pd.concat(dfs, ignore_index=True).sort_values(['Date', 'Time'], ascending=False)
            combined_df = combined_df.drop_duplicates(subset=['Date', 'Time'], keep='first').head(candles_count)
            
            # Update the market data with the complete candle data (historical + intraday + websocket)
            market_data['complete_candle_data'] = combined_df
            
        except KeyboardInterrupt:
            print("Shutting down...")  # Gracefully handle keyboard interrupt
            break
        except Exception as e:
            print(f"Error in fetch_candle_data: {e}")
            time.sleep(5)  # Wait before retrying to avoid rapid error loops
