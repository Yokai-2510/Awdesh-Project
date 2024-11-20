import requests as rq
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def fetch_candle_data(access_token, interval, instrument_key, n):
    def fetch_historical_candle_data(from_date, to_date, api_interval):
        url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/{api_interval}/{to_date}/{from_date}"
        headers = {
            'accept': 'application/json',
            'Api-Version': '2.0',
            'Authorization': f'Bearer {access_token}'
        }
        response = rq.get(url, headers=headers).json()
        
        if response.get('status') == 'error':
            print(f"API Error: {response}")
            return pd.DataFrame()
            
        candles_data = response.get('data', {}).get('candles', [])
        
        filtered_data = [[
            row[0],  # Datetime
            row[1],  # Open
            row[2],  # High
            row[3],  # Low
            row[4],  # Close
            row[5],  # Volume
            row[6]   # Open Interest
        ] for row in candles_data]
        
        columns = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest']
        df = pd.DataFrame(filtered_data, columns=columns)
        if not df.empty:
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            df = df.sort_values(by='Datetime')
        return df

    def fetch_intraday_candle_data(api_interval):
        url = f"https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/{api_interval}"
        headers = {
            'accept': 'application/json',
            'Api-Version': '2.0',
            'Authorization': f'Bearer {access_token}'
        }
        response = rq.get(url, headers=headers).json()
        
        if response.get('status') == 'error':
            print(f"API Error: {response}")
            return pd.DataFrame()
            
        candles_data = response.get('data', {}).get('candles', [])
        
        filtered_data = [[
            row[0],  # Datetime
            row[1],  # Open
            row[2],  # High
            row[3],  # Low
            row[4],  # Close
            row[5],  # Volume
            row[6]   # Open Interest
        ] for row in candles_data]
        
        columns = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest']
        df = pd.DataFrame(filtered_data, columns=columns)
        if not df.empty:
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            df = df.sort_values(by='Datetime')
        return df

    def resample_to_custom_interval(df, custom_interval):
        if df.empty:
            return df
            
        # Ensure datetime is set as index for resampling
        df = df.set_index('Datetime')
        
        # Define aggregation rules for custom interval candles
        agg_dict = {
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum',
            'OpenInterest': 'last'
        }
        
        # Resample to custom minute intervals
        resampled_df = df.resample(f'{custom_interval}min').agg(agg_dict)
        
        # Reset index to get Datetime back as a column
        resampled_df = resampled_df.reset_index()
        
        # Remove rows with NaN values (incomplete periods)
        resampled_df = resampled_df.dropna()
        
        # Split Datetime into Date and Time columns
        resampled_df['Date'] = resampled_df['Datetime'].dt.date
        resampled_df['Time'] = resampled_df['Datetime'].dt.strftime('%H:%M')
        
        # Drop the original Datetime column and reorder columns
        columns = ['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest']
        resampled_df = resampled_df[columns]
        
        return resampled_df
    
    # Parse the interval string to get the number of minutes
    if isinstance(interval, str):
        if 'minute' in interval.lower():
            custom_interval = int(interval.lower().replace('minute', '').strip())
        else:
            try:
                custom_interval = int(interval)
            except ValueError:
                raise ValueError("Invalid interval format. Use either 'Xminute' or just the number of minutes")
    else:
        try:
            custom_interval = int(interval)
        except ValueError:
            raise ValueError("Invalid interval format. Must be convertible to integer")

    # Get current date in India timezone
    india_tz = ZoneInfo("Asia/Kolkata")
    current_date = datetime.now(india_tz)

    # Calculate the date 10 days ago
    from_date = (current_date - timedelta(days=10)).strftime('%Y-%m-%d')
    to_date = current_date.strftime('%Y-%m-%d')

    print(f"Current time in India: {current_date}")
    print(f"Fetching data from {from_date} to {to_date}")

    # Always fetch 1-minute data from API for maximum flexibility
    api_interval = '1minute'

    # Fetch historical and intraday data
    df_historical = fetch_historical_candle_data(from_date, to_date, api_interval)
    df_intraday = fetch_intraday_candle_data(api_interval)

    # Combine the two DataFrames
    if df_historical.empty and df_intraday.empty:
        return pd.DataFrame()
    elif df_historical.empty:
        combined_df = df_intraday
    elif df_intraday.empty:
        combined_df = df_historical
    else:
        combined_df = pd.concat([df_historical, df_intraday], ignore_index=True)

    # Remove duplicates and sort
    if not combined_df.empty:
        combined_df = combined_df.drop_duplicates(subset='Datetime')
        combined_df = combined_df.sort_values(by='Datetime', ascending=True)
        
        # Resample to the custom interval
        combined_df = resample_to_custom_interval(combined_df, custom_interval)
        
        # Get the last 'n' candles
        last_n_candles = combined_df.tail(n)
        return last_n_candles
    
    return combined_df

# Example usage:
access_token = "your_access_token_here"
interval = 5  # Add Any Integer (minutes)
instrument_key = "NSE_INDEX|Nifty 50"
candles_count = 20

candle_data = fetch_candle_data(access_token, interval, instrument_key, candles_count)
print(candle_data)