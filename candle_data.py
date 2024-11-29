import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests
import time 

def fetch_historical_data(instrument_key):
    india_tz = ZoneInfo("Asia/Kolkata")
    current_date = datetime.now(india_tz)
    from_date = (current_date - timedelta(days=10)).strftime('%Y-%m-%d')
    to_date = current_date.strftime('%Y-%m-%d')
    
    url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/1minute/{to_date}/{from_date}"
    headers = {'accept': 'application/json', 'Api-Version': '2.0'}
    
    try:
        response = requests.get(url, headers=headers).json()
        candles_data = response.get('data', {}).get('candles', [])
        
        # Process historical data directly
        processed_rows = []
        for row in candles_data:
            dt = pd.to_datetime(row[0], errors='coerce')  # First element is datetime
            if pd.isna(dt):  # If parsing failed, skip this row
                continue
            processed_rows.append([dt.date(), dt.strftime('%H:%M'), float(row[1]), float(row[2]), float(row[3]), float(row[4])])
        
        df = pd.DataFrame(processed_rows, columns=['Date', 'Time', 'Open', 'High', 'Low', 'Close'])

        # Standardize DataFrame directly here
        if df.empty:
            return pd.DataFrame(columns=['Date', 'Time', 'Open', 'High', 'Low', 'Close'])
        
        # Standardize columns 
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        
        # Add datetime column for sorting
        df['Datetime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'])
        
        # Sort by datetime in descending order
        df = df.sort_values('Datetime', ascending=False)
        
        return df[['Date', 'Time', 'Open', 'High', 'Low', 'Close']]
    
    except Exception as e:
        print(f"Error fetching historical data: {e}")
        return pd.DataFrame()

def fetch_intraday_data(instrument_key):
    url = f"https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/1minute"
    headers = {'accept': 'application/json', 'Api-Version': '2.0'}
    
    try:
        response = requests.get(url, headers=headers).json()
        candles_data = response.get('data', {}).get('candles', [])
        
        # Process intraday data directly
        processed_rows = []
        for row in candles_data:
            dt = pd.to_datetime(row[0], errors='coerce')  # First element is datetime
            if pd.isna(dt):  # If parsing failed, skip this row
                continue
            processed_rows.append([dt.date(), dt.strftime('%H:%M'), float(row[1]), float(row[2]), float(row[3]), float(row[4])])
        
        df = pd.DataFrame(processed_rows, columns=['Date', 'Time', 'Open', 'High', 'Low', 'Close'])

        # Standardize DataFrame directly here
        if df.empty:
            return pd.DataFrame(columns=['Date', 'Time', 'Open', 'High', 'Low', 'Close'])

        # Standardize columns 
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        
        return df[['Date', 'Time', 'Open', 'High', 'Low', 'Close']]
    
    except Exception as e:
        print(f"Error fetching intraday data: {e}")
        return pd.DataFrame()


def fetch_websocket_data(market_data):
    # Convert to DataFrame from 'websocket_candle_data' in market_data
    df = market_data.get('websocket_candle_data')
    
    # Create a copy of the DataFrame to avoid modifying the original data
    df = df.copy()  
    
    # Ensure time format consistency:
    # Replace any space-separated time with colon-separated format
    df['Time'] = df['Time'].apply(lambda x: x.replace(" ", ":") if isinstance(x, str) else x)
    
    # Convert to datetime format, ensure format is %H:%M:%S and handle errors properly
    # Coerce errors to NaT and then format the datetime object to '%H:%M'
    df['Time'] = pd.to_datetime(df['Time'], format='%H:%M:%S', errors='coerce').dt.strftime('%H:%M')
    
    # Ensure the DataFrame has the required column names and order
    standard_columns = ['Date', 'Time', 'Open', 'High', 'Low', 'Close']
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    
    return df[standard_columns]

def fetch_candle_data(market_data):
    import pandas as pd
    import time

    instrument_key = "NSE_INDEX|Nifty 50"
    candles_count = 15
    time_interval = 7  # in minutes
    historical_candle_fetched = False
    while True:
        time.sleep(2)
        try:
            if historical_candle_fetched == False : 
                historical_df = fetch_historical_data(instrument_key)
                market_data['historical_candle_data'] = historical_df  # Update historical candle data
                historical_candle_fetched = True 
            else : historical_df = market_data['historical_candle_data']
            
            # Fetch intraday data
            intraday_df = fetch_intraday_data(instrument_key)
            market_data['intraday_candle_data'] = intraday_df  # Update intraday candle data

            # Fetch websocket data by passing market_data
            websocket_df = fetch_websocket_data(market_data)  # Fetch websocket data and format it

            # Combine historical, intraday, and websocket data
            dfs = [df for df in [historical_df, intraday_df, websocket_df] if not df.empty]
            if not dfs:
                continue

            # Combine all the data sources and clean up
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # Add datetime column for resampling
            combined_df['Datetime'] = pd.to_datetime(combined_df['Date'].astype(str) + ' ' + combined_df['Time'])
            combined_df = combined_df.sort_values('Datetime', ascending=False)
            combined_df = combined_df.drop_duplicates(subset=['Datetime'], keep='first')

            # Set datetime as index for resampling
            combined_df.set_index('Datetime', inplace=True)

            # Resample data to the specified time interval
            resampled_df = combined_df.resample(f'{time_interval}min').agg({
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last'
            }).dropna()

            # Take only the last candles_count candles
            market_data['complete_candle_data'] = resampled_df.tail(candles_count)

        except KeyboardInterrupt:
            print("Shutting down...")  # Gracefully handle keyboard interrupt
            break
        except Exception as e:
            print(f"Error in fetch_candle_data: {e}")
            time.sleep(5)  # Wait before retrying to avoid rapid error loops
