# The python package to archive Bigquery dataset into GCS bucket and restore it back

## Detailed Change History
[Change History of bigquery-archiver command](CHANGELOG.md)

## Python Design
### Archive / Restore task config
An archive or restore task can be defined in a YAML file, which is an element of a list of tasks. Each task is a dictionary
and processed by the python implementation
An example of such a config can be referred to:
[archive sample config](/resources/config/sample_archive_config.yaml)
[restore sample config](/resources/config/sample_restore_config.yaml)

## Config Design
The following table describes the common fields for both archive and restore task config.

| No. | Field                 | Type    | Description                                                                    |
|:----|:----------------------|:--------|:-------------------------------------------------------------------------------|
| 1   | `name`                | String  | A descriptive name for the task                                                |
| 2   | `concurrency`         | Integer | How many workers to process this task's entities, default is 1 (serial worker) |
| 2   | `task_type`           | String  | Either `archive` or `restore` to mark the task purpose                         |
| 3   | `continue_on_failure` | Boolean | Switch of control if the archive / restore should stop on failures.            |
| 4   | `overwrite_existing`  | Boolean | Switch of control if the existing entity should be deleted before restoring.   |

**Archive specific fields**:  

| No. | Field                     | Type     | Description                                          |
|:----|:--------------------------|:---------|:-----------------------------------------------------|
| 1   | `source_gcp_project_id`   | String   | The GCP project id for the source dataset            |
| 2   | `source_bigquery_dataset` | String   | The dataset name of the source dataset               |
| 3   | `destination_gcs_prefix`  | String   | The destination GCS prefix to hold archived entities |

**Restore specific fields**:  

| No. | Field                          | Type    | Description                                               |
|:----|:-------------------------------|:--------|:----------------------------------------------------------|
| 1   | `destination_gcp_project_id`   | String  | The GCP project id for the target dataset of restoring    |
| 2   | `destination_bigquery_dataset` | String  | The dataset name of the target dataset of restoring       |
| 3   | `source_gcs_archive`           | String  | The source GCS prefix which hosts the `dataset.json` file |
| 4   | `attach_archive_ts_to_label`   | Boolean | When true, archie_ts string added as label; Default true; |
| 5   | `skip_restore`                 | Dict    | When set, put true to entity names skip them in restore   |

## Supported Bigquery Entities and their fields in use
1. Table
   1. project_id
   2. dataset
   3. description
   4. labels
   5. schema_fields
   6. data_archive_format
   7. data_compression
   8. partition_config
2. Partitioned Table (implemented by `Table` entity)
   1. project_id
   2. dataset
   3. description
   4. labels
   5. schema_fields
   6. data_archive_format
   7. data_compression
   8. partition_config
3. View
   1. project_id
   2. dataset
   3. description
   4. labels
   5. schema_fields
   6. defining_query
4. Materialized View
   1. project_id
   2. dataset
   3. description
   4. labels
   5. schema_fields
   6. mview_query
   7. enable_refresh
   8. refresh_interval_seconds
5. Function
   1. project_id
   2. dataset
   3. description
   4. body
   5. arguments
   6. language
   7. return_type
6. Stored Procedure
   1. project_id
   2. dataset
   3. description
   4. body
   5. arguments
   6. language
   7. return_type
7. External Table
   1. project_id
   2. dataset
   3. description
   4. labels
   5. schema_fields
   6. partition_config 
   7. external_data_config

## Limitations
1. While restoring the entities having interdependencies, the restoring process only checks the completion of the previous task. In a case of failed requisites, the dependents will be restored anyway even if they are doomed to fail all the time.
2. When using `skip_restore`, be cautious it may break the DAG of view entities.
3. Body updating is not yet implemented for functions and stored procedures.
4. ~~AVRO datetime fields are limited to restore from files directly.~~ (RESOLVED by workaround in v1.4.3)

## Persistent data versioning
### metadata_version (used to track GCP Bigquery metadata changes)
1. `v1`
   1. introduced in 2025-02-23 and tested with GCP Bigquery by 2025-04-02
   2. supported from archiver version 1.4.0
### archiver_version (used to track the archiver implementation changes)
1. `v1`
   1. introduced in 2025-02-23 and tested with GCP Bigquery by 2025-04-02
   2. supported from archiver version 1.4.0