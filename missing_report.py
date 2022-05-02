import publiccomps_api
import db_connection
import time

CRAWL_DELAY = 0.3

def relevant_tickers(quarter_ends, dbc):
    quarter_count_dict = dbc.get_quarter_counts_dict()
    missing_metrics_dict = dbc.get_missing_metrics_dict()

    rel_tickers = list(missing_metrics_dict.keys())
    for ticker, quarters in quarter_ends.items():
        if ticker in quarter_count_dict and len(quarters) == quarter_count_dict[ticker]:
            continue
        rel_tickers.append(ticker)
    return rel_tickers

def missing_metrics(ticker):
    key_metrics = ["revenue"]
    qoq_metrics = ["subscription_revenue", "customers", "employees", "net_dollar_retention", "annual_recurring_revenue", "outstanding_shares"]
    missing_metrics = []
    ticker_quarters = publiccomps_api.ticker_quarters(ticker)
    if ticker_quarters:
        current_quarter = ticker_quarters[-1]
        for metric in key_metrics:
            if not current_quarter[metric]:
                missing_metrics.append(metric)
    if ticker_quarters and len(ticker_quarters) > 1:
        current_quarter = ticker_quarters[-1]
        previous_quarter = ticker_quarters[-2]
        for metric in qoq_metrics:
            if not current_quarter[metric] and previous_quarter[metric]:
                missing_metrics.append(metric)
    return missing_metrics

def error_report():
    quarter_ends = publiccomps_api.quarter_ends()
    dbc = db_connection.DBConnection()
    tickers = relevant_tickers(quarter_ends, dbc)
    for ticker in tickers:
        metrics = missing_metrics(ticker)
        if metrics:
            dbc.update_missing_metrics(ticker, metrics)
        else:
            dbc.delete_missing_metrics(ticker)
        dbc.update_quarter_counts(ticker, len(quarter_ends[ticker]))
        print(ticker + ": " + str(metrics))
        time.sleep(CRAWL_DELAY)
    dbc.close()

if __name__ == "__main__":
    error_report()
