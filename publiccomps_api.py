import requests
import os

url_base = 'https://publiccomps.com/api/v1/'
headers = {
    'X-User-Token': os.environ.get("PCOMP_KEY"),
    'X-User-Email': os.environ.get("PCOMP_EMAIL"),
    'Accept': 'application/json'
}

def ticker_quarters(ticker):
    r = requests.get(url_base + 'tickers/' + ticker, headers=headers)
    if r.status_code == 200:
        return r.json()

def quarter_data():
    r = requests.get(url_base + 'tickers/quarter_data', headers=headers)
    if r.status_code == 200:
        return r.json()
