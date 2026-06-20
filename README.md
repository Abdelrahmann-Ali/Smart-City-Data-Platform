# Smart City Real-Time Data Engineering Platform

A production-style real-time Smart City Data Engineering Platform designed to simulate intelligent transportation analytics for Cairo, Egypt using modern streaming, lakehouse, and cloud data engineering technologies.

The platform ingests live vehicle telemetry, weather, traffic, and road-event streams through Apache Kafka, processes them using Apache Spark Structured Streaming, stores them in an AWS S3 Medallion Lakehouse Architecture (Bronze / Silver / Gold), orchestrates workflows with Apache Airflow, loads analytical datasets into Snowflake, transforms warehouse models using dbt, and visualizes insights through Power BI and Grafana dashboards. Additionally, an integrated ML Pipeline performs feature engineering, model training, and predictive analytics for traffic forecasting and vehicle performance predictions.

---

# Architecture Overview
<img width="6529" height="4412" alt="smartcity_diagram" src="https://github.com/user-attachments/assets/0085f7f9-2839-4b11-92c9-a4adf10b1f48" />

## Batch + Real-Time Hybrid Architecture (Lambda-Style)

```text
Vehicle Simulator
        ↓
Apache Kafka
        ↓
────────────────────────────────────────
        REAL-TIME PATH
────────────────────────────────────────

Python Alert Consumer
        ↓
PostgreSQL (Operational Serving Layer)
        ↓
Grafana Dashboards

────────────────────────────────────────
        BATCH ANALYTICS PATH
────────────────────────────────────────

Spark Bronze Writer
        ↓
AWS S3 Bronze Layer

Spark Silver Cleaner
        ↓
AWS S3 Silver Layer

Spark Gold Aggregator
        ↓
AWS S3 Gold Layer
        ↓
Airflow Orchestration
        ↓
Snowflake Data Warehouse
        ↓
dbt Transformations
        ↓
Power BI Dashboards

────────────────────────────────────────
        ML PIPELINE PATH
────────────────────────────────────────
Feature Engineering 
        ↓
Model Training 
        ↓
Model Prediction 
        ↓
SQLite (ML Predictions Storage)
        ↓
Streamlit Dashboard 

```

---

# Project Objectives

- Simulate real-time smart transportation systems
- Process streaming telemetry data at scale
- Build a Medallion Lakehouse Architecture
- Implement distributed streaming ETL pipelines
- Design dimensional warehouse models
- Create operational monitoring dashboards
- Deliver business intelligence analytics
- Demonstrate modern enterprise data engineering architecture

---

# Technology Stack

| Layer | Technology |
|---|---|
| Programming | Python 3.11 |
| Streaming Platform | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Data Lake | AWS S3 |
| Storage Format | Apache Parquet |
| Workflow Orchestration | Apache Airflow |
| Data Warehouse | Snowflake |
| Transformations | dbt Core |
| Machine Learning | scikit-learn, XGBoost, pandas, numpy |
| Model Training | Custom Python pipelines |
| Operational Monitoring | Grafana |
| Metrics Collection | Prometheus |
| BI Analytics | Power BI |
| Containerization | Docker |
| Database | PostgreSQL |

---

# Data Sources

The project simulates and ingests multiple intelligent transportation data streams.

| Topic | Partitions | Retention | Rate | Purpose |
|---|---|---|---|---|
| vehicle-telemetry | 3 | 24h | 5 msg/sec | Main telemetry from 5 vehicles |
| weather-data | 1 | 24h | 1 msg/5min | Cairo weather snapshots |
| traffic-events | 2 | 24h | 5 msg/min | TomTom congestion data |
| road-events | 1 | 48h | ~1% of ticks | ACCIDENT / ROADWORK / BREAKDOWN |
| alerts | 1 | 7d | On anomaly | Real-time threshold violations |

---

# Real-Time Alert Engine

The platform continuously monitors streaming telemetry and generates alerts when operational thresholds are violated.

## Alert Thresholds

| Severity | Condition |
|---|---|
| 🔴 CRITICAL | Engine temperature > 105°C |
| 🟠 HIGH | Speed > 120 km/h |
| 🟠 HIGH | RPM > 5000 |
| 🟠 HIGH | Hard braking < -4.0 m/s² |
| 🟠 HIGH | Idle + temperature > 98°C |
| 🟡 MEDIUM | Fuel level < 10% |

---

# Medallion Lakehouse Architecture

## Bronze Layer
Raw immutable streaming ingestion from Kafka stored in Parquet format.

### Characteristics
- Raw event preservation
- Append-only
- Minimal transformation
- 7-day retention

---

## Silver Layer
Validated, cleaned, standardized streaming data.

### Transformations
- Schema enforcement
- Deduplication
- Null handling
- Data quality validation
- Type standardization

---

## Gold Layer
Business-level aggregated analytical datasets.

### Generated KPIs
- Fleet performance metrics
- Fuel consumption analytics
- Route efficiency
- Safety incident analytics
- Weather impact analysis
- Vehicle operational KPIs

---

# Why Apache Parquet?

The project uses Apache Parquet as the primary analytical storage format because it provides:

- Columnar storage optimization
- Compression efficiency
- Faster Spark query performance
- Reduced storage cost
- Predicate pushdown optimization
- Industry-standard lakehouse compatibility

Streaming events are transmitted in JSON format through Kafka and stored analytically as Parquet inside AWS S3.

---

# Data Warehouse & dbt

Snowflake is used as the cloud analytical warehouse layer.

dbt is responsible for:
- Dimensional modeling
- Star schema design
- Data marts
- Reusable analytical SQL transformations
- BI semantic modeling

## Warehouse Models

### Dimensions
- DIM_DATE
- DIM_VEHICLE
- DIM_ROUTE
- DIM_WEATHER_CONDITION
- DIM_ROAD_EVENT_TYPE

### Fact Tables
- MART_VEHICLE_PERFORMANCE
- MART_ROUTE_ANALYTICS
- MART_FUEL_ENVIRONMENT
- MART_INCIDENTS_SAFETY

---

# Airflow Orchestration

Apache Airflow orchestrates:
- Spark Gold jobs
- Snowflake loading
- dbt execution workflows
- Pipeline dependency management
- Batch scheduling

---

# Machine Learning Pipeline

The ML Pipeline provides predictive analytics and intelligent insights across the smart city platform. Data flows from the **AWS S3 Silver Layer** (validated, cleaned data) through feature engineering, model training, and predictive scoring.

## Data Source
The ML pipeline ingests cleaned and validated data from **AWS S3 Silver Layer**, including:
- Cleaned vehicle telemetry with standardized schemas
- Deduplicated traffic events
- Validated weather observations
- Processed road events

## ML Pipeline Components

### Feature Engineering 
Extracts and engineers advanced features from S3 Silver layer data:
- Vehicle performance metrics (speed, acceleration, heading)
- Traffic pattern features (congestion, vehicle density)
- Weather impact indicators (temperature, rain, visibility)
- Temporal aggregations (30-minute windowing)
- Statistical summaries and anomaly indicators

### Model Training 
Trains machine learning models using scikit-learn and XGBoost:
- Traffic volume forecasting (time series prediction)
- Vehicle performance prediction (health scoring)
- Fuel consumption estimation
- Incident/anomaly detection
- Route optimization recommendations

Supported algorithms:
- Linear Regression (baseline)
- XGBoost (gradient boosting)
- scikit-learn ensemble methods (random forest, gradient boosting)
- Custom preprocessing pipelines with MLflow tracking

### Model Prediction 
Generates real-time predictions and stores results in SQLite:
- Incident probability scoring per segment
- Vehicle health predictions
- Fuel efficiency estimates
- Dynamic route recommendations
- Anomaly scoring and threshold-based alerts

### Recommendation System
Intelligent recommendation engine that analyzes predictions to generate actionable insights:
- **Resource Deployment** - Deploy maintenance crews to high-risk segments
- **Route Diversion** - Suggest alternative routes based on traffic forecasts
- **Maintenance Alerts** - Schedule preventive maintenance for vehicle issues
- **Weather Alerts** - Issue warnings for weather-impacted segments
- Priority-based ranking (CRITICAL → HIGH → MODERATE → LOW)
- Confidence scoring and impact estimation

---

# Dashboards

## Power BI
Business Intelligence dashboards for:
- Fleet analytics
- Fuel efficiency
- Route performance
- Safety analysis
- Environmental insights

## Grafana
Operational real-time monitoring dashboards for:
- Live alerts
- Spark cluster health
- Streaming metrics
- System observability

## Streamlit ML Operations Dashboard 
Real-time ML pipeline monitoring and recommendation engine for:
- Live KPI metrics (predictions, alerts, alert rate, vehicle speed)
- 2-Hour predictive forecast with risk assessment
- Automated operational recommendations
- Segment vulnerability analysis with probability visualization
- Interactive explainability charts 

---

# Repository Structure

```text
Smart-City-Data-Platform/
│
├── airflow/
├── architecture/
├── dashboards/
│   ├── grafana/
│   └── powerbi/
├── dbt/
├── spark_jobs/
├── ML/
├── simulator/
├── docs/
├── screenshots/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# Key Engineering Concepts Demonstrated

- Distributed stream processing
- Real-time event-driven architecture
- Lambda-style hybrid architecture
- Medallion Lakehouse design
- Kafka partition strategy
- Spark Structured Streaming
- Cloud-native data engineering
- Dimensional warehouse modeling
- Infrastructure orchestration
- Operational observability
- Analytical BI modeling
- Machine learning feature engineering
- Predictive analytics pipeline
- Model training and evaluation
- End-to-end ML workflow integration

---

# Screenshots

## Architecture Diagram

<img width="6529" height="4412" alt="smartcity_diagram" src="https://github.com/user-attachments/assets/4ba6f00d-6ebd-4ef5-893b-db94f81e67fd" />

## Power BI Dashboard

<img width="1273" height="713" alt="Power_BI_Dashboard2" src="https://github.com/user-attachments/assets/f23ecf4f-b9c9-498a-902b-d0be62e4202c" />
<img width="1275" height="712" alt="Power_BI_Dashboard" src="https://github.com/user-attachments/assets/6a156258-8abe-436f-9006-bfc3c7231c46" />


## Grafana Monitoring

<img width="1917" height="1025" alt="Screenshot 2026-06-11 231231" src="https://github.com/user-attachments/assets/80b68378-ca1b-4a01-a89e-30ad5e6dd8c8" />

## Streamlit Dashboard

<img width="1794" height="887" alt="Streamlit_img" src="https://github.com/user-attachments/assets/a9306a5b-50cb-4949-974f-c8018f3e02cb" />
<img width="1814" height="842" alt="Streamlit_img2" src="https://github.com/user-attachments/assets/ae83678a-b6f2-4af6-a8bf-527a90de38c9" />


## Airflow DAG

<img width="1920" height="1019" alt="Screenshot (309)" src="https://github.com/user-attachments/assets/1efa42b1-dc97-4d63-8cec-fdc619bcbc04" />

---

# License

MIT License

---

# Author

Smart City Data Engineering Team

Built as a production-style end-to-end real-time lakehouse and analytics engineering platform.
