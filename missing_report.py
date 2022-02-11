import publiccomps_api

CRAWL_DELAY = 0.3

def update_db_entry(ticker, metrics):
    return

def delete_db_entry(ticker):
    return

def missing_metrics(ticker):
    key_metrics = ["revenue"]
    qoq_metrics = ["subscription_revenue", "customers", "employees", "net_dollar_retention", "annual_recurring_revenue"]
    missing_metrics = []
    ticker_quarters = pcomps_api.ticker_quarters(ticker)
    if ticker_quarters:
        current_quarter = ticker_quarters[-1]
        for metric in key_metrics:
            if not current_quarter[metric]:
                missing_metrics.append[metric]
    if ticker_quarters and len(ticker_quarters) > 1:
        current_quarter = ticker_quarters[-1]
        previous_quarter = ticker_quarters[-2]
        for metric in qoq_metrics:
            if not current_quarter[metric] and previous_quarter[metric]:
                missing_metrics.append(metric)
    return missing_metrics

def error_report():
    tickers = list(pcomps_api.quarter_data().keys())
    for ticker in tickers:
        metrics = missing_metrics(ticker)
        if metrics:
            update_db_entry(ticker, metrics)
        else:
            delete_db_entry(ticker)
        print(ticker + ": " + str(metrics))
        sleep(CRAWL_DELAY)
    return

if __name__ == "__main__":
    error_report()
