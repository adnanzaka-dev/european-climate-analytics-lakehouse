# European Weather & Climate Analytics Lakehouse

## Project Overview

This project implements an end-to-end data engineering pipeline for analyzing historical European weather and long-term climate trends using **Apache Spark, PySpark, Databricks, Delta Lake, and Medallion Architecture**.

Weather data is collected from the **Open-Meteo Historical Weather API**, using the ERA5 reanalysis model.

Phase 2 focuses on implementing the **Raw, Bronze, and Silver layers**, including:

- Full historical ingestion
- Incremental data loading
- Explicit schema enforcement
- Data cleaning and type casting
- Delta Lake MERGE operations
- Idempotent processing
- Parameterized backfill and recovery
- Schema-drift detection and quarantine testing
- Pipeline execution and error logging
- Data-quality validation

Gold-layer analytics and Power BI dashboards are planned for a later phase.

---

## 1. Project Scope

### Geographic Coverage

The pipeline covers nine European cities:

| Region | Cities |
|---|---|
| Northern Europe | Oslo, Stockholm, Helsinki, Copenhagen |
| Western Europe | London |
| Central Europe | Berlin |
| Southern Europe | Madrid, Rome |
| Eastern Europe | Warsaw |

### Time Coverage

| Load Type | Period | Purpose |
|---|---|---|
| Full Load | 1981–2020 | Build the initial historical dataset |
| Incremental Load | 2016–2025 | Test overlapping MERGE operations and add newer records |
| Final Silver Dataset | 1981–2025 | Complete analysis-ready dataset |

The incremental period overlaps with the Full Load for 2016–2020. This allows the pipeline to exercise the matched-record behavior of Delta MERGE without creating duplicate observations.

The period 2021–2025 extends the dataset with new records.

### Verified Production Results

| Metric | Result |
|---|---|
| Total Silver rows | 3,550,176 |
| Duplicate city–timestamp keys | 0 |
| Earliest timestamp | 1981-01-01 00:00:00 |
| Latest timestamp | 2025-12-31 23:00:00 |
| Production cities | 9 |
| Expected Full Load rows | 3,155,760 |
| Successful incremental Raw log entries | 90 |
| Successful incremental Bronze log entries | 90 |
| Successful incremental Silver log entries | 90 |

The final dataset's row count, date range, and duplicate-key results were validated directly against the production `silver_weather` Delta table.

The incremental execution-log summary was also checked in Databricks.

---

## 2. Technology Stack

| Technology | Purpose |
|---|---|
| Databricks | Development and execution environment |
| Apache Spark / PySpark | Distributed data processing |
| Delta Lake | ACID storage, MERGE and reliable tables |
| Python | API ingestion, validation and orchestration |
| Open-Meteo API | Historical weather source |
| ERA5 | Historical weather reanalysis model |
| GitHub | Source code and documentation |

---

## 3. Data Source

**Open-Meteo Historical Weather API**

Endpoint:

`https://archive-api.open-meteo.com/v1/archive`

API parameters include:

- latitude
- longitude
- start_date
- end_date
- hourly
- timezone=UTC
- models=era5

### Weather Variables

The source provides 12 hourly variables:

1. temperature_2m
2. relative_humidity_2m
3. apparent_temperature
4. precipitation
5. rain
6. snowfall
7. weather_code
8. pressure_msl
9. surface_pressure
10. cloud_cover
11. wind_speed_10m
12. wind_direction_10m

All production observations use hourly timestamps in UTC.

ERA5 is a reanalysis dataset; its values should not be described as direct measurements from individual weather stations.

---

## 4. Lakehouse Architecture

The pipeline uses the Medallion Architecture.

```text
Open-Meteo Historical Weather API
                |
                v
           Raw JSON
                |
                v
          Bronze Layer
     Nested Source Data
     + Ingestion Metadata
                |
                v
          Silver Layer
     Cleaned and Typed
     Hourly Observations
     + Delta MERGE
                |
                v
       Gold Layer (Future)
                |
                v
       Power BI (Future)
```

### Raw Layer

The Raw layer preserves API responses in JSON format.

It supports:

- Historical source preservation
- Batch-level traceability
- Reprocessing without unnecessary API calls
- Recovery from persisted upstream data

### Bronze Layer

The Bronze layer retains the nested API structure and attaches ingestion metadata.

### Silver Layer

The Silver layer transforms the nested hourly arrays into clean, typed, deduplicated observations suitable for analytical processing.

One Silver row represents one city at one hourly timestamp.

---

## 5. Bronze Data Model

**Table:** `bronze_weather`

**Logical key:** `batch_id`

| Column | Data Type | Description |
|---|---|---|
| latitude | DOUBLE | City latitude |
| longitude | DOUBLE | City longitude |
| generationtime_ms | DOUBLE | API generation time |
| utc_offset_seconds | INT | UTC offset |
| timezone | STRING | Timezone |
| timezone_abbreviation | STRING | Timezone abbreviation |
| elevation | DOUBLE | Elevation |
| hourly_units | STRUCT | Units metadata |
| hourly | STRUCT | Nested hourly observation arrays |
| city | STRING | City |
| country | STRING | Country |
| region | STRING | European region |
| source | STRING | Source identifier |
| batch_id | STRING | Logical batch identifier |
| load_type | STRING | FULL / INCREMENTAL / BACKFILL |
| raw_file_path | STRING | Original Raw JSON path |
| load_timestamp | TIMESTAMP | Bronze ingestion timestamp |

The nested `hourly` structure retains the source's hourly weather arrays.

The batch ID acts as a logical processing key rather than a database-enforced primary key constraint.

---

## 6. Silver Data Model

**Table:** `silver_weather`

**Logical primary key:** `city + timestamp`

| Column | Data Type | Description |
|---|---|---|
| city | STRING | City name |
| country | STRING | Country name |
| region | STRING | Geographic region |
| timestamp | TIMESTAMP | Hourly UTC timestamp |
| temperature_2m | DOUBLE | Temperature |
| relative_humidity_2m | DOUBLE | Humidity |
| apparent_temperature | DOUBLE | Apparent temperature |
| precipitation | DOUBLE | Precipitation |
| rain | DOUBLE | Rainfall |
| snowfall | DOUBLE | Snowfall |
| weather_code | INT | Weather condition code |
| pressure_msl | DOUBLE | Mean sea-level pressure |
| surface_pressure | DOUBLE | Surface pressure |
| cloud_cover | DOUBLE | Cloud cover |
| wind_speed_10m | DOUBLE | Wind speed |
| wind_direction_10m | DOUBLE | Wind direction |
| source | STRING | Source identifier |
| batch_id | STRING | Processing batch |
| load_type | STRING | Load category |
| raw_file_path | STRING | Original raw source path |
| bronze_load_timestamp | TIMESTAMP | Bronze ingestion time |
| load_timestamp | TIMESTAMP | Silver processing timestamp |

The city–timestamp combination is used by the Delta MERGE logic to prevent duplicate hourly observations.

---

## 7. Explicit Schema Enforcement

The pipeline defines schemas before reading source JSON.

PySpark types include:

```python
StructType
StructField
ArrayType
StringType
DoubleType
IntegerType
TimestampType
```

The implementation avoids relying on automatic schema inference.

Raw API responses are read using explicit nested schemas.

During Bronze-to-Silver transformation, source fields are converted into the correct target data types.

---

## 8. Bronze-to-Silver Transformation

The Silver transformation uses the following process:

```text
Nested Bronze Record
        |
        v
     arrays_zip
        |
        v
       explode
        |
        v
   Explicit Casting
        |
        v
   Data Validation
        |
        v
    Deduplication
        |
        v
   Delta Lake MERGE
        |
        v
    Silver Table
```

Major transformations include:

- Converting time strings into timestamps
- Casting weather measurements to numeric types
- Converting weather codes to integers
- Validating important fields
- Removing duplicate city–timestamp observations
- Preserving source and ingestion metadata

---

## 9. Full Historical Load

**Notebook:** `07_Run_Full_Pipeline.ipynb`

Configuration:

```text
Start Year: 1981
End Year: 2020
Cities: 9
Total City-Year Batches: 360
Expected Hourly Rows: 3,155,760
```

The production workflow processes city-year batches and writes data through Raw, Bronze and Silver.

It includes:

- API ingestion and retry handling
- Raw response validation
- Bronze ingestion
- Silver transformation
- Delta MERGE
- Execution logging
- Failure handling
- Resumable batch processing

A saved resumed Full Load execution reported:

```text
Successful batches: 225
Skipped batches: 135
Failed batches: 0
```

Skipped batches were already present and valid.

The completed historical dataset contains the expected 3,155,760 hourly observations for 1981–2020.

---

## 10. Incremental Data Loading

**Notebook:** `08_Incremental_Load_2016_2025.ipynb`

Incremental period:

```text
2016–2025
```

The implementation processes two conceptual periods:

| Period | MERGE Behavior |
|---|---|
| 2016–2020 | Existing city–timestamp records are matched |
| 2021–2025 | New city–timestamp records are inserted |

The incremental ingestion logs use these layer names:

```text
Raw
Bronze
Silver
```

Verified execution-log counts:

```text
INCREMENTAL / Raw / SUCCESS: 90
INCREMENTAL / Bronze / SUCCESS: 90
INCREMENTAL / Silver / SUCCESS: 90
```

Incremental batch IDs include the city, start date and end date.

After the incremental processing, the final Silver table contains:

```text
Rows: 3,550,176
Duplicate keys: 0
Start: 1981-01-01 00:00:00
End: 2025-12-31 23:00:00
```

---

## 11. Delta MERGE and Idempotency

**Notebook:** `05_MERGE_Incremental_and_Backfill.ipynb`

The Silver table is updated through Delta Lake MERGE.

Matching condition:

```text
target.city = source.city
AND target.timestamp = source.timestamp
```

MERGE behavior:

| Condition | Action |
|---|---|
| Matching key with changed data | UPDATE |
| New key | INSERT |
| Matching key with unchanged data | No unnecessary update |

### Controlled Testing

The notebook demonstrates MERGE behavior using a small dataset.

For safe repeatable testing, an isolated table can be used:

`silver_merge_demo`

The test checks:

- Row counts before and after MERGE
- Repeated MERGE processing
- Inserted-row counts
- Updated-row counts
- Duplicate city–timestamp keys
- Changed-value UPDATE behavior

The objective is to verify that processing the same input repeatedly does not create duplicate records or change the analytical result unnecessarily.

Production `silver_weather` should not be intentionally modified for demonstration-only testing.

---

## 12. Data Quality

The pipeline performs checks for:

- Required source fields
- Consistent nested array lengths
- Data-type conversion
- Null timestamps
- Duplicate city–timestamp combinations
- Missing ingestion timestamps
- Expected date coverage
- Expected observation counts

### Final Production Validation

```text
Silver Rows: 3,550,176
Duplicate Keys: 0
Earliest Timestamp: 1981-01-01 00:00:00
Latest Timestamp: 2025-12-31 23:00:00
```

The full-data validation queries read the actual production Delta table without overwriting it.

---

## 13. Schema Drift and Quarantine

**Notebook:** `06_Audit_Logging_and_Testing.ipynb`

The project includes the Delta table:

`schema_drift_quarantine`

The quarantine model contains:

| Column | Data Type |
|---|---|
| batch_id | STRING |
| raw_record | STRING |
| failure_reason | STRING |
| load_timestamp | TIMESTAMP |

### Controlled Schema-Drift Demonstration

The test provides four records:

- Two records with valid numeric values
- One record with an invalid numeric value
- One record with an unexpected source field

The validation separates valid records from rejected records.

Expected controlled-test result:

```text
Incoming: 4
Valid: 2
Quarantined: 2
```

Valid records are processed into an isolated test table, while invalid records are appended to the quarantine table with a failure reason.

**Implementation note:** This controlled test demonstrates detection and quarantine behavior. End-to-end schema drift support for the actual nested Open-Meteo production ingestion must also be verified in the production validation functions. A standalone test alone does not establish production integration.

---

## 14. Parameterized Backfill and Recovery

**Notebook:** `09_Backfill_Recovery.ipynb`

Backfill parameters:

```python
BACKFILL_CITY = "Berlin"
BACKFILL_START_DATE = "1995-01-01"
BACKFILL_END_DATE = "1995-12-31"
LOAD_TYPE = "BACKFILL"
```

The backfill workflow can reprocess historical data from persisted upstream layers.

### Recovery Path 1: Bronze to Silver

```text
Existing Bronze
       |
       v
Silver Transformation
       |
       v
Delta MERGE
       |
       v
Restored Silver
```

### Recovery Path 2: Raw to Bronze to Silver

```text
Persisted Raw JSON
       |
       v
Rebuild Bronze
       |
       v
Transform and MERGE
       |
       v
Restored Silver
```

An external API request is not required when the necessary Raw or Bronze records remain available.

### Controlled Missing-Data Recovery Test

The testing procedure uses Berlin observations from 1995.

The test copies the historical data into an isolated table:

`silver_backfill_demo`

It then removes January 1995 from that test table and runs the backfill against the isolated target.

Expected validation:

```text
Initial Berlin 1995 records: 8,760
Records after deletion: 8,016
Missing observations: 744
Expected restored records: 744
Expected final records: 8,760
Expected duplicate keys: 0
```

This procedure demonstrates actual recovery when its execution output confirms successful restoration.

The production Silver table is preserved throughout the controlled test.

---

## 15. Execution and Error Logging

The pipeline uses two operational tables:

```text
pipeline_execution_logs
pipeline_error_logs
```

### Execution Log Fields

| Column | Purpose |
|---|---|
| run_id | Unique execution identifier |
| batch_id | Batch identifier |
| layer | Pipeline processing layer |
| load_type | FULL / INCREMENTAL / BACKFILL / TEST |
| parameter_or_file | Input parameter or source |
| start_time | Execution start |
| end_time | Execution completion |
| status | SUCCESS / FAILURE |
| rows_inserted | Inserted rows |
| rows_updated | Updated rows |
| error_message | Error details |

The error-log table stores detailed information about failed operations.

Execution logs support:

- Processing traceability
- Batch monitoring
- Error investigation
- Recovery decisions
- Insert/update metric reporting

### Incremental Execution Evidence

The existing operational logs contain successful Raw, Bronze and Silver entries for incremental processing.

The pipeline uses the layer names `Raw`, `Bronze`, and `Silver` for production execution logs.

---

## 16. Fault Tolerance

The pipeline incorporates:

- API retry handling
- HTTP 429 response handling
- Exponential backoff
- Raw response validation
- Batch-level error logging
- Continue-on-failure processing
- Reusable persisted Raw data
- Resumable batch execution
- Delta MERGE-based updates
- Parameterized recovery

These features reduce unnecessary re-ingestion and support reliable processing of historical data.

---

## 17. Notebook Structure

| Notebook | Purpose |
|---|---|
| 01_Phase1_Data_Source_and_Samples.ipynb | Phase 1 data-source exploration |
| 02_Raw_API_Ingestion.ipynb | API ingestion demonstration |
| 03_Raw_to_Bronze.ipynb | Explicit Raw-to-Bronze transformation |
| 04_Bronze_to_Silver.ipynb | Silver transformation and casting |
| 05_MERGE_Incremental_and_Backfill.ipynb | MERGE and idempotency testing |
| 06_Audit_Logging_and_Testing.ipynb | Logging, quarantine and data-quality tests |
| 07_Run_Full_Pipeline.ipynb | Production Full Load |
| 08_Incremental_Load_2016_2025.ipynb | Production Incremental Load |
| 09_Backfill_Recovery.ipynb | Parameterized recovery workflow |

Notebooks 03–06 include demonstration and testing functionality.

Notebooks 07 and 08 perform production ingestion.

Notebook 09 provides the parameterized recovery workflow and controlled recovery testing.

---

## 18. Execution Guide

### Full Load

Run:

`07_Run_Full_Pipeline.ipynb`

This notebook processes the historical period 1981–2020.

**Important:** If the production tables already contain the completed Full Load, do not execute the entire ingestion loop again just to regenerate validation outputs.

Instead, run the read-only validation cells.

### Incremental Load

Run:

`08_Incremental_Load_2016_2025.ipynb`

Configure the intended start and end years before execution.

The notebook uses parameterized incremental batch processing.

For an existing completed pipeline, inspect saved execution logs and validate the final Silver dataset before initiating another ingestion.

### Backfill

Run:

`09_Backfill_Recovery.ipynb`

Set:

```python
BACKFILL_CITY
BACKFILL_START_DATE
BACKFILL_END_DATE
```

The selected period can be reprocessed using available persisted Raw or Bronze data.

For demonstration tests involving deletion, always use a separate test table.

### Validation Only

To inspect the existing production dataset without ingesting new data:

```python
from pyspark.sql.functions import col, min, max

silver = spark.table("silver_weather")

print("Rows:", silver.count())

print(
    "Duplicates:",
    silver.count()
    - silver.select("city", "timestamp").distinct().count()
)

silver.agg(
    min("timestamp").alias("start"),
    max("timestamp").alias("end")
).show()

display(silver.limit(20))
```

This code does not modify production data.

---

## 19. GitHub Repository Structure

```text
european-climate-analytics-lakehouse/
|
|-- README.md
|
|-- notebooks/
|   |-- 01_Phase1_Data_Source_and_Samples.ipynb
|   |-- 02_Raw_API_Ingestion.ipynb
|   |-- 03_Raw_to_Bronze.ipynb
|   |-- 04_Bronze_to_Silver.ipynb
|   |-- 05_MERGE_Incremental_and_Backfill.ipynb
|   |-- 06_Audit_Logging_and_Testing.ipynb
|   |-- 07_Run_Full_Pipeline.ipynb
|   |-- 08_Incremental_Load_2016_2025.ipynb
|   |-- 09_Backfill_Recovery.ipynb
|
|-- data/
|   |-- raw_full_sample/
|   |-- raw_incremental_sample/
|
|-- docs/
|   |-- Project_Proposal.docx
```

Folder and file names should match the actual submitted repository.

---

## 20. Phase 2 Rubric Coverage

| Requirement | Implementation / Evidence |
|---|---|
| Databricks environment | Databricks notebook workflows |
| GitHub repository | Source code and documentation |
| Bronze data model | Documented |
| Silver data model | Documented |
| Explicit schema | Implemented with StructType / StructField |
| Type casting | Bronze-to-Silver transformations |
| Bronze load_timestamp | Implemented |
| Silver load_timestamp | Implemented |
| Raw source tracking | Source paths and batch metadata |
| Deduplication | city + timestamp |
| Delta MERGE | Implemented |
| Idempotency | Controlled MERGE demonstration |
| Incremental processing | Production workflow and execution logs |
| Parameterized backfill | Implemented |
| Schema-drift quarantine | Controlled demonstration; production integration requires verification |
| Execution logs | Implemented |
| Error logs | Implemented |
| Full-data validation | 3,550,176 rows, zero duplicates |
| Saved execution evidence | To be preserved in final uploaded notebooks |

---

## 21. Phase 2 Deliverables

The submission includes:

1. GitHub repository containing PySpark notebooks.
2. Raw-to-Bronze and Bronze-to-Silver implementations.
3. Explicit data models and schema definitions.
4. Full Load and Incremental Load workflows.
5. Delta MERGE and idempotency demonstrations.
6. Parameterized backfill and recovery workflow.
7. Schema-drift and quarantine testing.
8. Pipeline execution and error logging.
9. README documentation.
10. Saved notebook outputs demonstrating the implementation.

---

## 22. Future Work

The next phase will extend the project with Gold-layer analytical tables.

Planned outputs include:

- Daily weather summaries
- Monthly climate aggregates
- Annual temperature and precipitation trends
- Extreme-weather indicators
- Climate comparisons between European cities
- Power BI dashboards

A proposed star schema will support downstream analytics using city and date dimensions and a central weather fact table.

---

## Conclusion

This project demonstrates a Databricks-based Lakehouse approach for processing multi-decade historical European weather data.

The implemented Raw, Bronze and Silver architecture supports historical ingestion, incremental loading, explicit schemas, typed transformations, MERGE operations, operational logging, and parameterized recovery.

The final validated production Silver dataset contains **3,550,176 hourly observations across nine European cities from 1981 through 2025, with zero duplicate city–timestamp keys**.

The project provides a foundation for future Gold-layer climate analytics and Power BI visualization.
