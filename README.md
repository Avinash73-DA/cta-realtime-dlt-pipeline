 🚍 CTA Real-Time Transit Analytics Pipeline using Databricks DLT

This project builds an end-to-end **real-time data pipeline** using the **Chicago Transit Authority (CTA)** Train & Bus Tracker APIs and historical CSV data. Leveraging **Delta Live Tables (DLT)** on **Databricks**, the pipeline processes streaming data, cleans it, applies quality checks, and generates insightful Gold-layer aggregations.

---

## 🧱 Project Structure

├── api/
│   ├── bus_streaming_api.py       # Script to fetch real-time CTA bus data
│   └── train_streaming_api.py     # Script to fetch real-time CTA train data
│
├── notebooks/
│   ├── bronze_dlt.py              # Bronze layer: ingest raw data into Delta Lake
│   ├── silver_dlt.py              # Silver layer: clean and transform data
│   └── gold_dlt.py                # Gold layer: aggregate data for analysis
│
├── resources/cta-samples/
│   ├── bus_data.json              # Sample real-time bus data
│   ├── train_data.json            # Sample real-time train data
│   └── ridership_historical.csv   # Historical ridership data (station-wise)
|
|__ pipeline.json
|
└── LICENSE
