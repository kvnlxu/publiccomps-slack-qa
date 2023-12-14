import json
import requests
import os
import db_connection

if __name__ == "__main__":
    dbc = db_connection.DBConnection()
    missing_metrics = dbc.get_recent_missing_metrics()
    qoq_percentages = dbc.get_recent_qoq_percentages()
    yoy_percentages = dbc.get_recent_yoy_percentages()
    dbc.close()
    if missing_metrics or qoq_percentages or yoy_percentages:
        response_text = "New errors found in the last 24 hours:\n\n"
        if missing_metrics:
            response_text += "Missing metrics for the latest quarter:\n\n"
            for ticker, metrics in missing_metrics:
                response_text += f"{ticker}: {metrics}\n"
        if qoq_percentages:
            response_text += "\n\n\n" + "Metrics with significant change over the last quarter:\n\n"
            for ticker, metrics in qoq_percentages:
                response_text += f"{ticker}: {metrics}\n"
        if yoy_percentages:
            response_text += "\n\n\n" + "Metrics with significant change over the last year:\n\n"
            for ticker, metrics in yoy_percentages:
                response_text += f"{ticker}: {metrics}\n"
        response_text += "\n\nMessage me using the /qa-full-report command for a full list of ongoing issues"

        requests.post('https://slack.com/api/chat.postMessage', {
            'token': os.environ.get("SLACK_BOT_TOKEN"),
            'channel': '#earnings-reminder',
            'text': response_text,
        }).json()
