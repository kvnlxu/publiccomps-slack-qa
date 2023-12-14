import os
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import db_connection

app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

@app.event("message")
def handle_message_events(body, logger):
    return

@app.command("/qa-full-report")
def full_report(ack, say, command):
    ack()
    dbc = db_connection.DBConnection()
    missing_metrics = dbc.get_missing_metrics()
    qoq_percentages = dbc.get_qoq_percentages()
    yoy_percentages = dbc.get_yoy_percentages()
    dbc.close()
    response_text = ""
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
    say(response_text)

@app.command("/qa-missing")
def missing(ack, say, command):
    ack()
    dbc = db_connection.DBConnection()
    missing_metrics = dbc.get_missing_metrics()
    dbc.close()
    response_text = "Missing metrics for the latest quarter:\n\n"
    for ticker, metrics in missing_metrics:
        response_text += f"{ticker}: {metrics}\n"
    say(response_text)

@app.command("/qa-qoq")
def qoq(ack, say, command):
    ack()
    dbc = db_connection.DBConnection()
    qoq_percentages = dbc.get_qoq_percentages()
    dbc.close()
    response_text = "Metrics with significant change over the last quarter:\n\n"
    for ticker, metrics in qoq_percentages:
        response_text += f"{ticker}: {metrics}\n"
    say(response_text)

@app.command("/qa-yoy")
def yoy(ack, say, command):
    ack()
    dbc = db_connection.DBConnection()
    yoy_percentages = dbc.get_yoy_percentages()
    dbc.close()
    response_text = "Metrics with significant change over the last year:\n\n"
    for ticker, metrics in yoy_percentages:
        response_text += f"{ticker}: {metrics}\n"
    say(response_text)

if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
