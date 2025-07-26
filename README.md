 🚍 CTA Real-Time Transit Analytics Pipeline using Databricks DLT

This project builds an end-to-end **real-time data pipeline** using the **Chicago Transit Authority (CTA)** Train & Bus Tracker APIs and historical CSV data. Leveraging **Delta Live Tables (DLT)** on **Databricks**, the pipeline processes streaming data, cleans it, applies quality checks, and generates insightful Gold-layer aggregations.

---

## 🧱 Project Structure

#### `api/`
This directory contains Python scripts responsible for fetching live data streams from the official CTA APIs.

-   **`bus_streaming_api.py`**: Connects to the CTA Bus Tracker API to continuously fetch and save real-time location, route, and vehicle information for the entire bus fleet.
-   **`train_streaming_api.py`**: Connects to the CTA Train Tracker API to fetch real-time arrival predictions, locations, and route information for all 'L' trains.

#### `notebooks/`
This directory holds the Delta Live Tables (DLT) notebooks that define the ETL/ELT pipeline. The pipeline is structured in a medallion architecture (Bronze, Silver, Gold) to ensure data quality and progressive refinement.

-   **`bronze_dlt.py`**: This is the first stage of the pipeline. It ingests the raw, unstructured, or semi-structured JSON/CSV data from the API scripts and historical files into Delta tables without applying major transformations. Its primary role is to create a durable, raw archive of the source data.
-   **`silver_dlt.py`**: This layer consumes data from the Bronze tables. Its responsibilities include cleaning the data (e.g., handling nulls, correcting data types), enriching it (e.g., joining with reference data), and conforming it to a well-defined schema. The output is a set of validated, queryable tables.
-   **`gold_dlt.py`**: The final layer of the pipeline. It takes the cleaned data from the Silver tables and aggregates it into business-level tables. These tables are optimized for analytics and reporting, often containing key performance indicators (KPIs), summaries, and other high-value insights (e.g., average wait times, station ridership trends).

#### `resources/cta-samples/`
This directory provides sample data files that can be used for development, testing, and demonstrating the pipeline's functionality without needing to connect to the live APIs.

-   **`bus_data.json`**: A JSON file containing a snapshot of the data returned by the bus streaming API.
-   **`train_data.json`**: A JSON file containing a snapshot of the data returned by the train streaming API.
-   **`ridership_historical.csv`**: A CSV file with historical daily ridership counts for each CTA station, useful for trend analysis.

### 🗺️ Pipeline Architecture

Below is the end-to-end architecture of the real-time CTA streaming pipeline implemented with Delta Live Tables in Databricks.

![CTA DLT Pipeline Architecture](resources/cta_dlt_pipeline.png)





















