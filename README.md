# Metadata Ingestion Pipeline using Microsoft Fabric

A reusable, metadata-driven ingestion pipeline built with Microsoft Fabric, OneLake, PySpark, and Delta Lake.

Instead of creating a separate pipeline for every source or table, this project uses one common ingestion pipeline controlled through metadata. New entities can be onboarded by adding configuration records rather than developing new end-to-end pipelines.

## Key Features

- Metadata-driven pipeline execution
- Reusable ingestion and processing logic
- SQL, REST API, CSV, JSON, and SFTP source support
- Full and incremental data loads
- Watermark-based change detection
- Bronze, Silver, and Gold architecture
- PySpark-based transformation engine
- Delta Lake append and merge operations
- Configurable data-quality rules
- Schema-drift detection
- Centralized logging and monitoring
- Quarantine handling for invalid records
- Safe and repeatable pipeline reruns

## Architecture

The solution is divided into five main components:

1. **Orchestration**

   Starts the pipeline manually or through a schedule, reads active entity configurations, and routes each entity through the reusable workflow.

2. **Metadata Control Plane**

   Stores source details, target mappings, processing methods, watermarks, transformation rules, quality checks, and operational settings.

3. **Ingestion**

   Extracts data from configured sources and stores timestamped source snapshots in the OneLake landing area.

4. **Processing**

   Uses reusable PySpark notebooks to apply schema alignment, transformations, data-quality rules, and Delta Lake writes.

5. **Operations**

   Records pipeline status, row counts, rejected records, schema hashes, watermarks, execution times, and error details.

## Medallion Architecture

### Landing

Stores timestamped source snapshots for traceability and recovery.

### Bronze

Stores raw Delta data with ingestion timestamps, run identifiers, source details, and other audit columns.

### Silver

Contains cleaned, validated, deduplicated, and conformed business data.

### Gold

Provides analytics-ready tables and aggregates for reporting and business intelligence consumption.

## Pipeline Flow

```text
Trigger or Schedule
        |
        v
Read Active Metadata
        |
        v
Select Full or Incremental Load
        |
        v
Extract and Land Source Data
        |
        v
Capture and Validate Schema
        |
        v
Apply Data-Quality Rules
        |
        v
Transform and Write to Delta
        |
        v
Build Gold Tables
        |
        v
Update Watermark and Log Results
```

## Repository Structure

```text
Metadata_Ingestion_Pipeline/
├── README.md
├── LICENSE
├── metadata/
│   ├── create_control_tables.sql
│   └── sample_entity_metadata.json
├── notebooks/
│   ├── 01_metadata_ingestion.py
│   └── 02_data_quality.py
├── pipelines/
│   └── pipeline_definition.json
├── sample_data/
│   └── customers.csv
├── docs/
│   ├── ARCHITECTURE.md
│   ├── OPERATIONS.md
│   └── SETUP.md
└── tests/
    └── test_metadata_contract.py
```

## Metadata Configuration

Each entity configuration defines how the reusable pipeline should process a source.

Example:

```json
{
  "entity_id": "customer",
  "active": true,
  "step_order": 10,
  "processing_method": "incremental",
  "source_type": "csv",
  "connection_key": "onelake_landing",
  "source_object": "customers.csv",
  "staging_path": "Files/landing/customer/{run_id}/",
  "target_lakehouse": "migration_lh",
  "target_table": "silver_customer",
  "write_mode": "merge",
  "merge_keys": [
    "customer_id"
  ],
  "watermark": {
    "column": "updated_at",
    "datatype": "timestamp",
    "value": "1900-01-01T00:00:00Z"
  },
  "quality_rules": [
    {
      "type": "not_null",
      "column": "customer_id",
      "severity": "error",
      "action": "quarantine"
    }
  ]
}
```

## Pipeline Parameters

The reusable pipeline can accept the following parameters:

| Parameter | Required | Description |
|---|---:|---|
| `run_id` | Yes | Unique identifier for the pipeline execution |
| `trigger_name` | Yes | Name of the manual or scheduled trigger |
| `table_id` | No | Runs a specific entity when supplied |
| `load_type` | No | Overrides the configured full or incremental load type |

## Data-Quality Rules

The framework can apply configurable rules such as:

- Not-null validation
- Duplicate detection
- Accepted-value validation
- Datatype validation
- Primary-key validation
- Custom business rules

Records that fail critical rules can be written to a quarantine area with the run ID, entity ID, failed rule, and rejection reason.

## Incremental Processing

Incremental loads use a configured watermark column and the last successfully committed value.

The watermark is updated only after:

1. Data has been written successfully to Delta Lake.
2. Quality results have been recorded.
3. Operational metrics have been committed.

If processing fails, the existing watermark is preserved so the pipeline can be rerun safely.

## Logging and Monitoring

Each execution can record:

- Run ID
- Entity ID
- Start and completion timestamps
- Execution status
- Source rows read
- Target rows written
- Rejected row count
- Previous and new watermark values
- Schema hash
- Error details
- Execution duration

## Getting Started

### Prerequisites

Before running the project, ensure you have:

- Access to a Microsoft Fabric workspace
- A Fabric Lakehouse
- Permission to create Fabric pipelines and notebooks
- Source-system connection details
- Access to OneLake storage

### Setup Instructions

1. Create a Microsoft Fabric workspace and Lakehouse.
2. Run `metadata/create_control_tables.sql`.
3. Load `metadata/sample_entity_metadata.json`.
4. Upload the sample data to the OneLake landing area.
5. Import the PySpark notebooks into Fabric.
6. Create the pipeline using `pipelines/pipeline_definition.json` as a reference.
7. Configure the required Fabric connections.
8. Run the pipeline with a unique `run_id`.
9. Validate the Bronze, Silver, and Gold outputs.
10. Review the operational logs and data-quality results.

## Pipeline Execution

The orchestration pipeline performs the following activities:

1. Receives the trigger name, run ID, and optional table ID.
2. Reads active entity configurations.
3. Filters the metadata when a specific table ID is supplied.
4. Loops through the selected entities.
5. Determines whether each entity requires a full or incremental load.
6. Extracts and lands the source data.
7. Captures the source schema.
8. Runs the reusable PySpark processing notebook.
9. Applies transformations and data-quality rules.
10. Writes valid records to Delta Lake.
11. Writes invalid records to quarantine storage.
12. Records operational metrics.
13. Updates the watermark after successful completion.
14. Returns the final pipeline status.

## Success and Failure Handling

### Successful Execution

When an entity completes successfully, the pipeline:

- Commits the Delta Lake write
- Records row counts and execution metrics
- Stores the latest schema hash
- Updates the watermark
- Marks the entity as successful

### Failed Execution

When an entity fails, the pipeline:

- Preserves the previous watermark
- Retains the staged source data
- Records the failed activity and error details
- Marks the entity as failed
- Propagates the error to the parent pipeline
- Allows the same logical batch to be rerun safely

## Data-Quality Processing

The reusable quality notebook separates records into two outputs:

### Valid Records

Records that satisfy all configured quality rules are written to the target Delta table.

### Rejected Records

Records that fail one or more critical quality rules are written to a quarantine area.

A rejected record can include:

- Run ID
- Entity ID
- Source filename
- Failed rule
- Rejection reason
- Rejection timestamp
- Original record values

## Delta Lake Write Modes

The pipeline supports configurable write modes.

### Append

Adds incoming records to the target table.

### Merge

Uses configured business keys to update matching records and insert new records.

### Overwrite

Replaces the target data. This mode should be used carefully and only for approved full-load scenarios.

## Watermark Management

The watermark configuration contains:

- Watermark column
- Watermark datatype
- Previous committed value
- Current candidate value
- Incremental-load predicate
- Last successful run ID

The candidate watermark must not replace the committed watermark until the processing transaction succeeds.

## Schema Management

The framework can capture and compare source schemas between executions.

A schema hash can be used to detect:

- New columns
- Removed columns
- Datatype changes
- Nullable-property changes
- Column-order changes

Schema changes can be logged, allowed, or treated as failures according to project requirements.

## Security Considerations

- Do not store passwords or access tokens in metadata files.
- Use Fabric connections or a managed secret store.
- Apply least-privilege permissions to workspaces and Lakehouses.
- Restrict access to operational logs if they contain sensitive error details.
- Use separate development, testing, and production configurations.
- Do not commit production connection information to GitHub.

## Testing

The repository contains a lightweight metadata-contract test.

The test validates:

- Required metadata fields
- Supported write modes
- Project naming
- Sample configuration structure

Run the test with:

```bash
python -m pytest tests/
```

## Technology Stack

- Microsoft Fabric
- Fabric Data Factory
- OneLake
- Fabric Lakehouse
- PySpark
- Delta Lake
- SQL
- Python
- JSON

## Sample Data

All sample data and metadata in this repository are synthetic.

The project does not contain:

- Client information
- Production credentials
- Confidential source data
- Production connection strings
- Proprietary business rules

## Benefits

- Reduces duplicate pipeline development
- Simplifies onboarding of new entities
- Standardizes data-quality processing
- Improves operational visibility
- Supports controlled incremental ingestion
- Enables safer recovery and reruns
- Separates configuration from processing logic
- Creates a scalable foundation for migration projects

## Future Enhancements

- Microsoft Fabric deployment pipelines
- Automated metadata onboarding
- Schema-evolution policies
- Advanced source-to-target reconciliation
- Microsoft Teams or email notifications
- Power BI operational monitoring dashboard
- CI/CD validation for metadata changes
- Parallel execution controls
- Dependency-driven entity processing
- Configurable retry policies

## Author

**Akash Nath**

