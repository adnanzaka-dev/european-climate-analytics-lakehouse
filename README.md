\# European Weather \& Climate Change Analytics Lakehouse



\## Project Overview



This project develops an end-to-end data engineering pipeline for analyzing historical weather and long-term climate trends across major European cities.



The project follows the Medallion Architecture and processes data through Bronze, Silver, and Gold layers using Apache Spark and Databricks. The final analytical data will be used to build a Power BI dashboard for climate trend analysis.



\## Team Members



| Name | Roll Number |

|------|-------------|

| Muhammad Adnan Zaka | 24I-2537 |

| Ahmad Azad | 24L-2523 |



\*\*Course:\*\* Data Analysis \& Visualization (DAV)  

\*\*University:\*\* FAST-NUCES Lahore  

\*\*Semester:\*\* Fall 2026



\## Data Source



Historical weather data is obtained from the Open-Meteo Historical Weather API using the ERA5 dataset.



Production Scope:

\- 20 major European cities

\- Hourly weather observations

\- Historical period: 1940–2025

\- Timezone: UTC



\## Weather Variables



The dataset includes:



\- Temperature

\- Relative Humidity

\- Apparent Temperature

\- Precipitation

\- Rain

\- Snowfall

\- Weather Code

\- Mean Sea Level Pressure

\- Surface Pressure

\- Cloud Cover

\- Wind Speed

\- Wind Direction



City, country, region, and timestamp metadata are also included.



\## Ingestion Strategy



The pipeline supports two ingestion patterns.



\### Full Load



The initial Full Load will ingest historical weather data from 1940 to 2025 for the selected 20 European cities.



\### Incremental Load



After the Full Load, only newly available weather observations will be ingested.



A watermark/checkpoint based on the last successfully processed timestamp will be used to determine the next incremental data range.



This avoids repeatedly processing the complete historical dataset and reduces unnecessary computation.



\## Sample Data



Phase 1 contains two representative sample datasets.



\### Full Load Sample



\- 5 representative European cities

\- Full year of 2024

\- 43,920 rows

\- 16 columns

\- Approximately 15.79 MB



\### Incremental Load Sample



\- Same 5 cities

\- January 1–7, 2025

\- 840 rows

\- 16 columns

\- Approximately 0.302 MB



The five sample cities are London, Oslo, Berlin, Madrid, and Warsaw.



\## Estimated Production Volume



The complete production dataset is estimated to contain approximately:



\- 15,077,760 hourly observations

\- 5.29 GB of raw JSON data



Expected incremental volume:



\- Daily: 480 rows

\- Weekly: 3,360 rows

\- Monthly: approximately 14,400 rows

\- Yearly: approximately 175,320 rows



\## Medallion Architecture



\### Bronze Layer



The Bronze layer stores data close to its original source format along with ingestion metadata such as source, city, ingestion timestamp, batch ID, and load type.



\### Silver Layer



The Silver layer prepares the data for analysis by:



\- Flattening the source data

\- Enforcing schema

\- Converting data types

\- Handling null values

\- Removing duplicates

\- Performing data quality checks

\- Standardizing fields and units



\### Gold Layer



The Gold layer will contain business-ready analytical datasets and aggregations such as:



\- Daily weather summaries

\- Monthly climate summaries

\- Annual climate trends

\- Extreme weather indicators



\## Data Analysis \& Visualization Concepts



The project follows an iterative Data Analysis Lifecycle.



Data Preparation is used to make the data valid, consistent, and analysis-ready.



Exploratory Data Analysis (EDA) will be used to discover patterns, identify anomalies, and formulate analytical hypotheses.



Apache Spark provides distributed data processing, while Delta Lake provides the reliable storage/table layer for the Lakehouse architecture.



\## Dashboard Plan



Power BI will be used to visualize:



\- Annual average temperature trends

\- Temperature changes across decades

\- Annual and seasonal precipitation

\- Hot-day frequency

\- Heavy-rainfall observations

\- Extreme temperature indicators



The dashboard will support filtering by city, country, region, year, and decade.



\## Technology Stack



\- Python

\- Open-Meteo Historical Weather API

\- Apache Spark / PySpark

\- Databricks

\- Delta Lake

\- Power BI

\- Git \& GitHub



\## Repository Structure



```text

european-climate-analytics-lakehouse/

├── README.md

├── data/

│   ├── full\_load\_sample\_5\_cities\_2024.json

│   └── incremental\_load\_sample\_5\_cities\_2025\_01\_01\_to\_07.json

├── notebooks/

│   └── 01\_Phase1\_Data\_Source\_and\_Samples.ipynb

└── docs/

&#x20;   └── Phase\_1\_Proposal\_European\_Climate\_Analytics\_Final.docx

