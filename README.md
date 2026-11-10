# Metadata Ingestion Pipeline

A reference implementation of a reusable, metadata-controlled ingestion pipeline for Microsoft Fabric. Instead of building one pipeline per source table, the orchestration layer reads entity metadata and routes each active entity through the same extraction, quality, Delta write, logging, and recovery flow.

## Architecture

Sources (SQL, REST, CSV/JSON, SFTP) are orchestrated into timestamped OneLake landing paths, then processed through Bronze, Silver, and Gold layers. PySpark handles schema alignment, transformations, data-quality rules, and append/merge writes. Control tables hold source, target, watermark, quality, transformation, and operational settings.

## Repository layout

- `metadata/` - control-table DDL and sample entity configuration
- `notebooks/` - reusable PySpark ingestion and quality logic
- `pipelines/` - parameter contract and activity blueprint
- `sample_data/` - synthetic source data
- `docs/` - architecture, operations, and setup notes
- `tests/` - lightweight metadata contract checks

## Quick start

1. Create the metadata tables using `metadata/create_control_tables.sql`.
2. Load `metadata/sample_entity_metadata.json` into the entity configuration store.
3. Upload `sample_data/customers.csv` to a Fabric Lakehouse landing area.
4. Import the notebooks and configure the pipeline using `pipelines/pipeline_definition.json`.
5. Run with `trigger_name`, `run_id`, and optionally `table_id`.
6. Confirm metrics, schema hash, and watermark in the operations tables.

## Design guarantees

- Watermarks advance only after successful Delta writes.
- Failed runs retain staged data and do not lose incremental state.
- Quality rejects are quarantined with a reason.
- Run IDs connect source counts, writes, rejects, errors, and duration.
- Synthetic examples contain no client data or credentials.

## Technology

Microsoft Fabric, OneLake, Fabric Data Factory, PySpark, Delta Lake, and SQL.

## License

MIT - see `LICENSE`.
