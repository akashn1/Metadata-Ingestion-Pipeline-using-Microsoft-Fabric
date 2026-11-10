-- Metadata Ingestion Pipeline control plane (reference DDL)
CREATE TABLE md.entity_config (
  entity_id VARCHAR(100) PRIMARY KEY, active BIT, step_order INT,
  processing_method VARCHAR(30), source_type VARCHAR(30), connection_key VARCHAR(100),
  source_object VARCHAR(256), staging_path VARCHAR(512), target_lakehouse VARCHAR(100),
  target_table VARCHAR(128), write_mode VARCHAR(20), merge_keys VARCHAR(500),
  watermark_column VARCHAR(128), watermark_datatype VARCHAR(30), watermark_value VARCHAR(256)
);

CREATE TABLE md.quality_rule (
  rule_id VARCHAR(100) PRIMARY KEY, entity_id VARCHAR(100), rule_type VARCHAR(30),
  column_name VARCHAR(128), rule_value VARCHAR(1000), severity VARCHAR(20), action VARCHAR(20)
);

CREATE TABLE md.pipeline_run (
  run_id VARCHAR(100), entity_id VARCHAR(100), status VARCHAR(20), started_at DATETIME2,
  completed_at DATETIME2, rows_read BIGINT, rows_written BIGINT, rows_rejected BIGINT,
  schema_hash VARCHAR(128), error_message VARCHAR(4000), PRIMARY KEY (run_id, entity_id)
);
