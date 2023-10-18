import publiccomps_api
import db_connection
import time

CRAWL_DELAY = 0.3

def relevant_tickers(quarter_ends, dbc):
    quarter_count_dict = dict(dbc.get_quarter_counts())
    missing_metrics_dict = dict(dbc.get_missing_metrics())
    qoq_percentages_dict = dict(dbc.get_qoq_percentages())
    yoy_percentages_dict = dict(dbc.get_yoy_percentages())

    rel_tickers = missing_metrics_dict.keys() | qoq_percentages_dict.keys() | yoy_percentages_dict.keys()
    for ticker, quarters in quarter_ends.items():
        if ticker in quarter_count_dict and len(quarters) == quarter_count_dict[ticker]:
            continue
        rel_tickers.add(ticker)
    return rel_tickers

def missing_metrics(ticker_quarters):
    key_metrics = ["revenue"]
    qoq_metrics = ["subscription_revenue", "customers", "employees", "net_dollar_retention",
        "annual_recurring_revenue", "outstanding_shares", "free_cash_flow", "gross_margin"]
    missing_metrics = []
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

def qoq_percent_metrics(ticker_quarters):
    tolerance = {
        "outstanding_shares": 0.1,
        "employees": 0.1,
        "customers": 0.1,
        "free_cash_flow": 0.1
    }
    percent_metrics = []
    if ticker_quarters and len(ticker_quarters) > 1:
        current_quarter = ticker_quarters[-1]
        previous_quarter = ticker_quarters[-2]
        for metric, error_limit in tolerance.items():
            if current_quarter[metric] and previous_quarter[metric]:
                error_rate = (current_quarter[metric] - previous_quarter[metric]) / previous_quarter[metric]
                if abs(error_rate) >= error_limit:
                    percent_metrics.append(metric + ": " + str(round(error_rate, 3)))
    return percent_metrics

def yoy_percent_metrics(ticker_quarters):
    tolerance = {
        "revenue": 0.75,
        "cost_of_revenue": 0.75,
        "research_development": 0.75,
        "sales_marketing": 0.75,
        "general_admin": 0.75,
        "free_cash_flow": 0.75
    }
    percent_metrics = []
    if ticker_quarters and len(ticker_quarters) > 4:
        current_quarter = ticker_quarters[-1]
        previous_quarter = ticker_quarters[-5]
        for metric, error_limit in tolerance.items():
            if current_quarter[metric] and previous_quarter[metric]:
                error_rate = (current_quarter[metric] - previous_quarter[metric]) / previous_quarter[metric]
                if abs(error_rate) >= error_limit:
                    percent_metrics.append(metric + ": " + str(round(error_rate, 3)))
    return percent_metrics

def error_report():
    quarter_ends = publiccomps_api.quarter_ends()
    dbc = db_connection.DBConnection()
    tickers = relevant_tickers(quarter_ends, dbc)
    for ticker in tickers:
        ticker_quarters = publiccomps_api.ticker_quarters(ticker)
        missed_metrics = missing_metrics(ticker_quarters)
        qoq_percentages = qoq_percent_metrics(ticker_quarters)
        yoy_percentages = yoy_percent_metrics(ticker_quarters)
        if missed_metrics:
            dbc.update_missing_metrics(ticker, missed_metrics)
        else:
            dbc.delete_missing_metrics(ticker)
        if qoq_percentages:
            dbc.update_qoq_percentages(ticker, qoq_percentages)
        else:
            dbc.delete_qoq_percentages(ticker)
        if yoy_percentages:
            dbc.update_yoy_percentages(ticker, yoy_percentages)
        else:
            dbc.delete_yoy_percentages(ticker)
        dbc.update_quarter_counts(ticker, len(quarter_ends[ticker]))
        print(f"{ticker}: {missed_metrics}, {qoq_percentages}, {yoy_percentages}")
        time.sleep(CRAWL_DELAY)
    dbc.close()

if __name__ == "__main__":
    error_report()
