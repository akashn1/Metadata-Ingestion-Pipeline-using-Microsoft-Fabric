# Fabric setup

1. Create a Fabric workspace and Lakehouse.
2. Create the control tables and load the sample entity metadata.
3. Configure source connections and map logical connection keys per environment.
4. Import both notebooks and replace reference paths with workspace paths.
5. Recreate the pipeline activities from `pipelines/pipeline_definition.json`.
6. Upload the sample CSV, run a manual test, and verify Bronze/Silver output and logs.
7. Configure schedules, alerts, access controls, and retention before production use.
