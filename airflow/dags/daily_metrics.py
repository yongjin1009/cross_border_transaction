from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone, time
from clickhouse_connect import get_client
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

class MetricProcessor:
    def __init__(self):
        import logging
        self.log = logging.getLogger(__name__)
        self.client = get_client(
            host='clickhouse',
            port=8123,
            username='admin',
            password='admin', 
            database='finance'      
        )

    def insert_data(self, table, columns, data):
        '''
        table = table name
        columns = list of column name
        data = list of list, each inner list is a row with values correspond to the order of columns
        '''
        try:
            self.client.insert(
                table = table,
                data=data,
                column_names=columns
                )
        except Exception as e:
            log.info(f"Error insert data: {e}")

    def query_transac(self, start, end):
        query = f"""
                SELECT * FROM transaction
                WHERE timestamp between {start} and {end}
                """
        df = self.client.query_df(query)
        df['country'] = df['country'].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
        df['destination_country'] = df['destination_country'].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
        df['is_cross_border'] = df['is_cross_border'].astype(bool)
        return df

    def query_anomaly(self, start, end):
        query = f"""
                SELECT * FROM anomaly
                WHERE timestamp between {start} and {end}
                """
        df = self.client.query_df(query)
        return df

    def cal_daily_metrics(self, df_anomaly, df_transac, date):
        total_transac = len(df_transac)
        total_anomaly = len(df_anomaly)
        total_amount = df_transac['amount'].sum()
        total_cross_border_transac = len(df_transac[df_transac['is_cross_border']==True])
        unique_sender = df_transac['sender_id'].nunique()

        columns = ['date','total_transactions','total_cross_border_transac','total_amount','anomaly_count','unique_senders']
        data = []
        data.append([date, total_transac, total_cross_border_transac, total_amount, total_anomaly, unique_sender])

        self.insert_data('daily_metrics', columns, data)

    def cal_sender_summary(self, df_anomaly, df_transac, date):
        merge_df = pd.merge(df_anomaly, df_transac, on='transaction_id', how='inner')
        data = []
        
        for sender in df_transac['sender_id'].unique():
            total_transactions = len(df_transac[df_transac['sender_id'] == sender])
            total_anomalies = len(merge_df[merge_df['sender_id'] == sender])
            risk_score = total_anomalies / total_transactions
            data.append([sender, total_transactions, total_anomalies, risk_score, date])
                
        columns = ['sender_id', 'total_transactions', 'total_anomalies', 'risk_score', 'last_updated']
        self.insert_data('sender_risk_score', columns, data)


def main():
    now = datetime.now(timezone.utc)
    
    start_dt = datetime.combine(now.date(), time(0,0,0, tzinfo=timezone.utc))
    end_dt = datetime.combine(now.date(), time(23, 59, 59, tzinfo=timezone.utc))

    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())

    mp = MetricProcessor()
    df_anomaly = mp.query_anomaly(start_ts, end_ts)
    df_transac = mp.query_transac(start_ts, end_ts)

    mp.cal_daily_metrics(df_anomaly, df_transac, start_dt.date())
    mp.cal_sender_summary(df_anomaly, df_transac, start_dt)
    

with DAG(
    dag_id='daily_transaction_summary',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    summary_task = PythonOperator(
        task_id='summarize_transactions',
        python_callable=main
    )
