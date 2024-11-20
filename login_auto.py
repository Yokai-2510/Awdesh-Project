from playwright.sync_api import sync_playwright, expect, TimeoutError
from urllib.parse import parse_qs, urlparse, quote
import pyotp
import requests
import json
import time

def fetch_access_token(credentials_file='credentials.json'):
    # Load credentials from the JSON file
    with open(credentials_file, 'r') as file:
        credentials = json.load(file)
    
    API_KEY = credentials["API_KEY"]
    SECRET_KEY = credentials["SECRET_KEY"]
    RURL = credentials["RURL"]
    TOTP_KEY = credentials["TOTP_KEY"]
    MOBILE_NO = credentials["MOBILE_NO"]
    PIN = credentials["PIN"]
    
    rurlEncode = quote(RURL, safe="")
    auth_code = None

    AUTH_URL = f'https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri={rurlEncode}'

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True,
            args=['--disable-web-security', '--disable-features=IsolateOrigins,site-per-process']
        )
        
        context = browser.new_context(
            ignore_https_errors=True,
            bypass_csp=True
        )
        
        page = context.new_page()

        try:
            # Set up request listener
            print("Started Automatic Retrieval of Access Token")
            def handle_request(request):
                nonlocal auth_code
                url = request.url
                if RURL in url and 'code=' in url:
                    print(f"Captured request URL: {url}")
                    auth_code = parse_qs(urlparse(url).query)['code'][0]
                    print(f"Captured authorization code: {auth_code}")

            page.on('request', handle_request)

            print("Navigating to login page...")
            page.goto(AUTH_URL, wait_until='networkidle')
            
            print("Filling mobile number...")
            page.locator("#mobileNum").fill(MOBILE_NO)
            page.get_by_role("button", name="Get OTP").click()
            
            page.wait_for_selector("#otpNum", state="visible")
            
            print("Generating and filling OTP...")
            otp = pyotp.TOTP(TOTP_KEY).now()
            page.locator("#otpNum").fill(otp)
            page.get_by_role("button", name="Continue").click()
            
            page.wait_for_selector("input[type='password']", state="visible")
            
            print("Filling PIN...")
            page.get_by_label("Enter 6-digit PIN").fill(PIN)
            
            print("Clicking continue and waiting for navigation...")
            
            # Click continue and ignore any navigation errors
            try:
                with page.expect_navigation(timeout=5000):
                    page.get_by_role("button", name="Continue").click()
            except TimeoutError:
                print("Navigation timeout as expected, proceeding with captured code...")
            
            time.sleep(2)  # Small delay to ensure code capture

        except Exception as e:
            print(f"Browser automation error: {str(e)}")
            if auth_code is None:  # Only raise if we didn't get the auth code
                raise
        finally:
            context.close()
            browser.close()

    if not auth_code:
        raise Exception("Failed to obtain authorization code")

    print(f"Proceeding with authorization code: {auth_code}")
    print("Exchanging authorization code for access token...")
    
    token_url = 'https://api-v2.upstox.com/login/authorization/token'
    headers = {
        'accept': 'application/json',
        'Api-Version': '2.0',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    data = {
        'code': auth_code,
        'client_id': API_KEY,
        'client_secret': SECRET_KEY,
        'redirect_uri': RURL,
        'grant_type': 'authorization_code'
    }

    try:
        response = requests.post(token_url, headers=headers, data=data)
        response.raise_for_status()
        access_token = response.json().get('access_token')
        
        if not access_token:
            print("Response content:", response.text)
            raise Exception("No access token in response")

        with open('access_token.txt', 'w') as token_file:
            token_file.write(access_token)

        print(f"Access token successfully saved to 'access_token.txt'")
        return access_token

    except requests.exceptions.RequestException as e:
        print(f"Error during token exchange. Status code: {e.response.status_code if hasattr(e, 'response') else 'N/A'}")
        print(f"Response content: {e.response.text if hasattr(e, 'response') else 'N/A'}")
        raise Exception(f"Failed to exchange authorization code for access token: {str(e)}")

if __name__ == "__main__":
    try:
        access_token = fetch_access_token()
        print("Login successful!")
    except Exception as e:
        print(f"Error during login process: {str(e)}")