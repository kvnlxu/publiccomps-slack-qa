import os
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import db_connection

app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

@app.event("message")
def handle_message_events(body, logger):
    return

@app.command("/report")
def report(ack, say, command):
    ack()
    dbc = db_connection.DBConnection()
    missing_metrics = dbc.get_missing_metrics()
    dbc.close()
    response_text = "These are the missing metrics for the latest quarter:\n\n"
    for ticker, metrics in missing_metrics:
        response_text += f"{ticker}: {metrics}\n"
    say(response_text)

if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
