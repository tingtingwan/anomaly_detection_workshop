# Anomaly Detection Workshop

## Quick Start

Replace placeholders: `{CATALOG_NAME}`, `{SCHEMA_NAME}`, `{USERNAME}` with your values.

## Two Independent Workflows

### **DLT Pipeline** (Real-time)
1. `00_synthetic_data_generator.ipynb` - Generate demo data
2. `dlt_pipeline/` - Streaming: JSON → Bronze → Silver → Gold
3. `BONUS-ai-query-anomaly-detection.ipynb` - AI explanations

### **ML Training** (Batch)
1. `01_feature_engineering.ipynb` - Feature store
2. `02_training_and_tracking.ipynb` - Model training
3. `03_serving_batch_inference.ipynb` - Batch predictions

## Usage Options

| **Option** | **Components** | **Use Case** |
|------------|----------------|--------------|
| **DLT Only** | `00_` + DLT + BONUS | Real-time streaming |
| **ML Only** | `01_` + `02_` + `03_` | Batch ML training |
| **Combined** | All components | End-to-end solution |

## Table Schema (Medallion Architecture)

| **Layer** | **Table** | **Purpose** |
|-----------|-----------|-------------|
| Bronze | `bronze_customer_events` | Raw JSON events |
| Silver | `silver_customer_features` | Engineered features |
| Gold | `gold_batch_predictions` | Final predictions |

## Prerequisites

- Unity Catalog workspace
- MLflow Model Registry access
- DLT pipeline permissions
- Volume creation permissions

---
*Documentation minimized - verbose setup instructions removed from notebooks and README*
