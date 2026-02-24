# Travel Data Databricks Project

An end-to-end data engineering pipeline built on Databricks to process, clean, and analyze personal travel and health data. This project serves as a demonstration of modern data engineering practices using PySpark, Delta Live Tables, Databricks Asset Bundles, and Unity Catalog.

All primary ETL code and asset configurations are located inside the `travel_bundle` directory.

## 🛠️ Technology Stack & Features

* **Compute & Processing:** Databricks, PySpark (DataFrames)
* **Orchestration:** Delta Live Tables (DLT) for declarative pipeline management
* **Data Governance & Storage:** Unity Catalog Volumes (modern, secure file storage avoiding deprecated DBFS) and Delta Lake
* **Deployment:** Databricks Asset Bundles (DABs) for CI/CD and Infrastructure as Code (IaC)
* **Testing:** Pytest for local code validation
* **Development Environment:** VS Code with Databricks Connect and Ruff formatting

## 📊 Data Sources

The pipelines ingest multiple raw file formats (JSON, CSV) into Unity Catalog Volumes. The data domains include:
* **Fitbit Health Metrics:** Heart rate, sleep scores, and daily steps.
* **Location Data:** Google Timeline raw JSON exports.
* **Travel Records:** Flight logs and manual travel logs.
* **Financials:** Transaction records.

## 🚀 Getting Started

To run this project in your own Databricks environment (including Databricks Community Edition), follow these steps:

### 1. Cluster Setup
* Create a Databricks compute cluster. If you are using the Community Edition, select the available standard runtime.

### 2. Data Ingestion (Unity Catalog Volumes)
* **Note:** This project adheres to Databricks best practices by explicitly avoiding the deprecated DBFS (Databricks File System).
* In your Databricks workspace, navigate to **Catalog**.
* Create a Volume under your chosen catalog and schema (e.g., `/Volumes/workspace/travel/travel_data_volume/`).
* Manually upload your raw CSV and JSON files into their respective subdirectories (`fitbit/`, `flight_logs/`, `google_timeline/`, etc.) within the Volume.

### 3. Deployment & Execution
This project uses Databricks Asset Bundles (DABs).

1. Install the Databricks CLI and authenticate with your workspace.
2. Navigate to the `travel_bundle` directory:
   ```bash
   cd travel_bundle
