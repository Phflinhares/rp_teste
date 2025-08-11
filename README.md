# CDI Bonus Calculation - Data Product

## Description

Project to calculate the CDI Bonus, which computes the daily interest on users' wallet balances based on captured movements (CDC) and daily CDI rates.

This project uses Apache Spark for distributed processing of data stored in Parquet files.

---

## Project Structure

- `rp_teste/`  
  Source code of the pipeline, including:  
  - `main.py` — main pipeline  
  - `process_history.py` — processes wallet history  
  - `calc_interest.py` — calculates daily interest  

- `rp_teste/data/`  
  Contains input data:  
  - `parquet files` — Parquet files with movements (CDC)  
  - `daily_rates.parquet` — Parquet file with daily CDI rates  

- `rp_teste/data/output_daily_payouts/`  
  Contains output data:  
  - `output_daily_payouts/` — calculated interest payout output  

---

## Prerequisites

- Python 3.8+  
- Apache Spark 3.x  
- Java JDK 8 or higher  
- Hadoop configured on Windows (only if running on Windows) with `winutils.exe`  

---

## Quick explanation

- `pyspark`: to run Spark with Python  
- `pandas` and `numpy`: to generate rate files and manipulate local data  
- `pyarrow`: to read/write Parquet files in pandas


### RECOMMENDATIONS
In a production database environment, I recommend implementing an incremental CDC (Change Data Capture) process that consistently analyzes the latest recorded file and inserts the new data accordingly.

I would structure the project into Bronze, Silver, and Gold layers: ingesting raw data in the Bronze layer, performing data cleansing and transformation in the Silver layer, and delivering the finalized DataFrame in the Gold layer.

Since this is a production process, I believe it is essential to establish a comprehensive data quality framework to monitor potential input and output errors, as well as to enforce business rules.