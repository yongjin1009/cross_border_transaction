<h3>Transaction ETL Pipeline</h3>
This project is a real-time and batch ETL pipeline that simulates transaction data ingestion, anomaly detection, and daily summarization using:

- Apache Kafka for streaming
- Apache Flink (Java, 1.17) for real-time processing
- ClickHouse for analytical storage
- Apache Airflow for daily batch summarization
- Docker Compose orchestration to simplify deployment

📌 Features
💸 Real-time processing: Stream transaction events from Kafka and sink valid records to ClickHouse via Flink.

🚨 Anomaly detection: Identify suspicious transactions and flag them to a dedicated anomalies table.

📊 Daily metrics: Use Airflow to generate daily summaries from ClickHouse.

🐳 Fully containerized: Everything runs via Docker Compose.

🧱 Architecture <br>
Kafka -> Flink (Java) -> ClickHouse -> Airflow (daily summaries)

✅ TODO / Future Work
- Flink ML for more robust fraud detection
- Add Grafana for dashboarding

<h3>Setup</h3>
1. git clone https://github.com/yongjin1009/cross_border_transaction.git<br>
2. build Apache_Flink into jar file<br>
3. cd cross_border_transaction<br>
4. docker cp Apache_Flink\target\Apache_Flink-1.0.jar flink_jobmanager:/job.jar<br>
5. docker compose up --build -d<br>
6. docker exec -it flink_jobmanager flink run -c org.yongjin.job.TransactionProcessing /job.jar<br>
7. Run producer.py in kafka folder to simulate transaction data 
