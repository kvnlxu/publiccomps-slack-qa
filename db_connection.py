import os
import psycopg2
from urllib.parse import urlparse

class DBConnection:

    def __init__(self, db_url=os.environ.get("PC_SLACK_DB_URL")):
        parsed_url =  urlparse(db_url)
        params = {
            'database': 'd45naqprfovler',
            'user': parsed_url.username,
            'password': parsed_url.password,
            'host': parsed_url.hostname,
            'port': 5432
        }
        self.con = psycopg2.connect(**params)
        self.cur = self.con.cursor()

    def update_quarter_counts(self, ticker, count):
        insert_sql = '''
            INSERT INTO quarter_counts (ticker, quarter_count)
            VALUES (%s, %s)
            ON CONFLICT (ticker) DO UPDATE SET
            quarter_count = EXCLUDED.quarter_count;
        '''
        self.cur.execute(insert_sql, (ticker, count))

    def get_quarter_counts(self):
        query = 'SELECT ticker, quarter_count FROM quarter_counts'
        self.cur.execute(query)
        results = self.cur.fetchall()
        return results

    def update_missing_metrics(self, ticker, metrics):
        insert_sql = '''
            INSERT INTO missing_metrics (ticker, metrics)
            VALUES (%s, %s)
            ON CONFLICT (ticker) DO UPDATE SET
            metrics = EXCLUDED.metrics;
        '''
        self.cur.execute(insert_sql, (ticker, str(metrics)))

    def delete_missing_metrics(self, ticker):
        delete_sql = '''
            DELETE FROM missing_metrics
            WHERE ticker=%s;
        '''
        self.cur.execute(delete_sql, (ticker,))

    def get_missing_metrics(self):
        query = 'SELECT ticker, metrics FROM missing_metrics'
        self.cur.execute(query)
        results = self.cur.fetchall()
        return results

    def update_qoq_percentages(self, ticker, metrics):
        insert_sql = '''
            INSERT INTO qoq_percentages (ticker, metrics)
            VALUES (%s, %s)
            ON CONFLICT (ticker) DO UPDATE SET
            metrics = EXCLUDED.metrics;
        '''
        self.cur.execute(insert_sql, (ticker, str(metrics)))

    def delete_qoq_percentages(self, ticker):
        delete_sql = '''
            DELETE FROM qoq_percentages
            WHERE ticker=%s;
        '''
        self.cur.execute(delete_sql, (ticker,))

    def get_qoq_percentages(self):
        query = 'SELECT ticker, metrics FROM qoq_percentages'
        self.cur.execute(query)
        results = self.cur.fetchall()
        return results

    def update_yoy_percentages(self, ticker, metrics):
        insert_sql = '''
            INSERT INTO yoy_percentages (ticker, metrics)
            VALUES (%s, %s)
            ON CONFLICT (ticker) DO UPDATE SET
            metrics = EXCLUDED.metrics;
        '''
        self.cur.execute(insert_sql, (ticker, str(metrics)))

    def delete_yoy_percentages(self, ticker):
        delete_sql = '''
            DELETE FROM yoy_percentages
            WHERE ticker=%s;
        '''
        self.cur.execute(delete_sql, (ticker,))

    def get_yoy_percentages(self):
        query = 'SELECT ticker, metrics FROM yoy_percentages'
        self.cur.execute(query)
        results = self.cur.fetchall()
        return results

    def close(self):
        self.con.commit()
        self.con.close()
