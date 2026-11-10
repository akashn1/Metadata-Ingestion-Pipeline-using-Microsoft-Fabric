# Operations and recovery

- Record `run_id`, entity, timestamps, status, counts, schema hash, and error text.
- Update the watermark only after the Delta transaction and metrics succeed.
- Retain failed staging paths for inspection and rerun with the same logical batch.
- Quarantine invalid rows with rule and reason; do not silently discard them.
- Alert on failure, unexpected zero rows, schema drift, and reject thresholds.
- Keep secrets in Fabric connections or a managed secret store, never in metadata files.
