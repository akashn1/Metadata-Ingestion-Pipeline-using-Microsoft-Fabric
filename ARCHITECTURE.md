# Metadata Ingestion Pipeline architecture

The pipeline separates orchestration, metadata, ingestion, processing, and operations. A trigger creates a run ID, loads active entity configurations, and routes each entity through a common execution path. Sources land as timestamped files in OneLake. Bronze retains raw history and audit columns; Silver applies schema, quality, conformance, and merge logic; Gold exposes analytics-ready aggregates.

The control plane stores orchestration order, source and target definitions, watermarks, transformations, quality rules, datastore mappings, and operational results. Configuration drives execution, so onboarding is a metadata change rather than a new end-to-end pipeline.
