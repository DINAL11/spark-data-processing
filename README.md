# Scalable Data Processing with PySpark

## Overview
A PySpark pipeline that processes over 1M records to generate insights and train a machine learning model.

## Features
- ETL on large-scale synthetic data
- Random Forest classifier to predict purchase behavior
- Optimized DAGs with Spark transformations
- Data saved in Parquet format

## Run Steps

```bash
# Step 1: Generate data
python data_generator.py

# Step 2: Run ETL pipeline
spark-submit etl_pipeline.py

# Step 3: Train model
spark-submit model_training.py
