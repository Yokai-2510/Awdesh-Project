import asyncio
import json
import ssl
import upstox_client
import websockets
from google.protobuf.json_format import MessageToDict
import MarketDataFeed_pb2 as pb
import pandas as pd
import requests as rq
import nest_asyncio
from datetime import datetime, timezone, timedelta
nest_asyncio.apply()    # Enable nested event loops

# Initialize global market data dictionary
market_data = {
    'nifty_spot_price': None,
    'websocket_candle_data': pd.DataFrame(),
    'nifty_option_chain': pd.DataFrame()
}

def start_websocket(data_dict):
    def get_access_token():
        with open('access_token.txt', 'r') as file:
            return file.read().strip()

    def initialize_market_data():   #   Initialize required market data and return instrument keys list
        
        def get_open_value(access_token):
            url = "https://api.upstox.com/v2/market-quote/quotes"
            headers = {
                'accept': 'application/json',
                'Api-Version': '2.0',
                'Authorization': f'Bearer {access_token}'
            }
            response = rq.get(url, headers=headers, params={'symbol': "NSE_INDEX|Nifty 50"})
            return response.json()['data']['NSE_INDEX:Nifty 50']['ohlc']['open']

        def create_options_df(open_value):
            strike_price_cap = 1000
            rounded_open = round(open_value / 50) * 50
            strike_range = (rounded_open - strike_price_cap, rounded_open + strike_price_cap)
            
            # Read the instruments data
            instruments_df = pd.read_csv("https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz")
            
            # Filter for the relevant options
            options_df = instruments_df[
                (instruments_df['exchange'] == 'NSE_FO') &
                (instruments_df['instrument_type'] == 'OPTIDX') &
                (instruments_df['lot_size'] == 25) &
                (instruments_df['option_type'].isin(['CE', 'PE']))
            ]
            
            # Find the upcoming Thursday
            today = datetime.today()
            days_until_thursday = (3 - today.weekday()) % 7  # 3 is Thursday (0=Monday, 6=Sunday)
            upcoming_thursday = today + timedelta(days=days_until_thursday)
            upcoming_thursday_str = upcoming_thursday.strftime('%Y-%m-%d')  # Format: 2024-11-28
            
            data_dict['nifty_option_chain'] = options_df[
                (options_df['expiry'] == upcoming_thursday_str) &
                (pd.to_numeric(options_df['strike']) >= strike_range[0]) &
                (pd.to_numeric(options_df['strike']) <= strike_range[1])
            ][['instrument_key', 'strike', 'option_type', 'expiry']]
            
            return data_dict['nifty_option_chain']['instrument_key'].tolist()

        access_token = get_access_token()
        open_value = get_open_value(access_token)
        instrument_keys = create_options_df(open_value)
        instrument_keys.append("NSE_INDEX|Nifty 50")
        
        return access_token, instrument_keys


    def process_nifty_spot(nifty_data): #   Extract Nifty 50 spot price from websocket data
        
        if nifty_data:
            data_dict['nifty_spot_price'] = nifty_data.get("ff", {}).get("indexFF", {}).get("ltpc", {}).get("ltp")

    def process_nifty_candles(nifty_data):  #   Process Nifty 50 candle data, convert to IST
        
        if not nifty_data:  return
        IST_OFFSET = timedelta(hours=5, minutes=30)
        candles = []

        ohlc_data = nifty_data.get("ff", {}).get("indexFF", {}).get("marketOHLC", {}).get("ohlc", [])
        for candle in ohlc_data:
            if candle.get("interval") == "I1" and candle.get("ts"):
                timestamp = int(candle["ts"])
                ist_dt = (datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc) + IST_OFFSET)
                candles.append({
                    "Date": ist_dt.strftime('%Y-%m-%d'),
                    "Time": ist_dt.strftime('%H:%M:%S'),
                    "Open": candle.get("open"),
                    "High": candle.get("high"),
                    "Low": candle.get("low"),
                    "Close": candle.get("close"),})
        
        if candles: data_dict['websocket_candle_data'] = pd.DataFrame(candles)
            


    def process_options_chain(feeds_data):  #   Process options chain data from websocket feed
        
        for key, data in feeds_data.items():
            if key != "NSE_INDEX|Nifty 50" and data:
                market_ff = data.get("ff", {}).get("marketFF", {})
                ltpc = market_ff.get("ltpc", {})
                greeks = market_ff.get("optionGreeks", {})
                bid_ask = market_ff.get("marketLevel", {}).get("bidAskQuote", [{}])[0]
                ohlc = market_ff.get("marketOHLC", {}).get("ohlc", [{}])[0]
                feed_details = market_ff.get("eFeedDetails", {})
                
                # Find the row index for this instrument key
                idx = data_dict['nifty_option_chain'].index[
                    data_dict['nifty_option_chain']['instrument_key'] == key
                ]
                
                if len(idx) > 0:
                    # Update values directly in the dataframe
                    data_dict['nifty_option_chain'].loc[idx, 'LTP'] = ltpc.get("ltp")
                    data_dict['nifty_option_chain'].loc[idx, 'Delta'] = greeks.get("delta")
                    data_dict['nifty_option_chain'].loc[idx, 'Theta'] = greeks.get("theta")
                    data_dict['nifty_option_chain'].loc[idx, 'Gamma'] = greeks.get("gamma")
                    data_dict['nifty_option_chain'].loc[idx, 'Vega'] = greeks.get("vega")
                    data_dict['nifty_option_chain'].loc[idx, 'IV'] = greeks.get("iv")
                    data_dict['nifty_option_chain'].loc[idx, 'Best_Bid_Price'] = bid_ask.get("bp")
                    data_dict['nifty_option_chain'].loc[idx, 'Best_Ask_Price'] = bid_ask.get("ap")
                    data_dict['nifty_option_chain'].loc[idx, 'Volume'] = ohlc.get("volume")
                    data_dict['nifty_option_chain'].loc[idx, 'OI'] = feed_details.get("oi")
                    data_dict['nifty_option_chain'].loc[idx, 'POI'] = feed_details.get("poi")

    async def run_market_data_websocket():  #   Main function to run websocket connection and process market data"""
        
        access_token, instrument_keys = initialize_market_data()
        
        # Setup websocket connection
        configuration = upstox_client.Configuration()
        configuration.access_token = access_token
        response = upstox_client.WebsocketApi(
            upstox_client.ApiClient(configuration)
        ).get_market_data_feed_authorize('2.0')
        
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        async with websockets.connect(
            response.data.authorized_redirect_uri,
            ssl=ssl_context
        ) as websocket:
            await websocket.send(json.dumps({
                "guid": "someguid",
                "method": "sub",
                "data": {
                    "mode": "full",
                    "instrumentKeys": instrument_keys
                }
            }).encode('utf-8'))
            
            while True:
                try:
                    message = await websocket.recv()
                    feed_data = MessageToDict(pb.FeedResponse().FromString(message))
                    feeds = feed_data.get("feeds", {})
                    # print("\nfeeds : \n", feeds)
                    
                    # Process each data type - directly updating data_dict
                    process_nifty_spot(feeds.get("NSE_INDEX|Nifty 50"))
                    process_nifty_candles(feeds.get("NSE_INDEX|Nifty 50"))
                    process_options_chain(feeds)
                    
                except Exception as e:
                    print(f"Error in websocket processing: {e}")
                    await asyncio.sleep(1)

    loop = asyncio.new_event_loop() #   Function to run in separate thread
    asyncio.set_event_loop(loop)
    try:    loop.run_until_complete(run_market_data_websocket())
    except Exception as e:  print(f"Websocket thread error: {e}")
    finally:    loop.close()

if __name__ == "__main__":
    start_websocket()
