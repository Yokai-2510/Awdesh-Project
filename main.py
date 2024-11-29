#   main.py


import pandas as pd
import threading
import nest_asyncio
import os
from datetime import datetime, timezone, timedelta
import time
from login_auto import fetch_access_token
from websocket import start_websocket 
from candle_data import fetch_candle_data
nest_asyncio.apply()    #   Enable nested event loops


# Initialize data dictionary with default values
market_data = {
    'nifty_spot_price': None,
    'websocket_candle_data': pd.DataFrame(),
    'complete_candle_data': pd.DataFrame(),
    'historical_candle_data': pd.DataFrame(),
    'intraday_candle_data': pd.DataFrame(),
    'nifty_option_chain': pd.DataFrame()    }



if __name__ == "__main__":


    #   Fetch Access Token using Auto Login 
    fetch_access_token(credentials_file='credentials.json') 


    # Start websocket in a separate thread
    market_data_thread = threading.Thread(target=start_websocket, args=(market_data,))
    market_data_thread.daemon = True  # Set as daemon thread
    market_data_thread.start()
    time.sleep(3)


    # Candle Data Thread
    candle_data_thread = threading.Thread(target=fetch_candle_data,args=(market_data,))
    candle_data_thread.daemon = True
    candle_data_thread.start()


    try:
        while True:
            os.system('cls' if os.name == 'nt' else 'clear')
            #   print(f"\nNifty Spot: {market_data['nifty_spot_price']}")
            #   print(f"\nOptions Chain Data \n: {market_data['nifty_option_chain']}")
            print(f"\n Complete Candles Data : \n{market_data['complete_candle_data']}")
            #   print(f"\n websocket Candles Data : \n{market_data['websocket_candle_data']}")
            #   print(f"\n Intraday Candles Data : \n{market_data['intraday_candle_data']}")
            #   print(f"\n Historical Candles Data : \n{market_data['historical_candle_data']}")
            time.sleep(3)

    except KeyboardInterrupt:   print("Shutting down...")
