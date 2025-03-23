# The python package to archive Bigquery dataset into GCS bucket and restore it back

## Python Design
### Archive / Restore task config
A archive or restore task can be defined in a YAML file, which is an element of a list of tasks. Each task is a dictionary
and processed by the python implementation
An example of such config can be referred to:
[archive sample config](/resources/config/sample_archive_config.yaml)
[restore sample config](/resources/config/sample_restore_config.yaml)

## Config Design
Following table describes the common fields for both archive and restore task config.

| No. | Field                 | Type    | Description                                                                    |
|:----|:----------------------|:--------|:-------------------------------------------------------------------------------|
| 1   | `name`                | String  | A descriptive name for the task                                                |
| 2   | `concurrency`         | Integer | How many workers to process this task's entities, default is 1 (serial worker) |
| 2   | `task_type`           | String  | Either `archive` or `restore` to mark the task purpose                         |
| 3   | `continue_on_failure` | Boolean | Switch of control if the archive / restore should stop on failures.            |

**Archive specific fields**:  

| No. | Field                     | Type     | Description                                          |
|:----|:--------------------------|:---------|:-----------------------------------------------------|
| 1   | `source_gcp_project_id`   | String   | The GCP project id for the source dataset            |
| 2   | `source_bigquery_dataset` | String   | The dataset name of the source dataset               |
| 3   | `destination_gcs_prefix`  | String   | The destination GCS prefix to hold archived entities |

**Restore specific fields**:  

| No. | Field                          | Type     | Description                                               |
|:----|:-------------------------------|:---------|:----------------------------------------------------------|
| 1   | `destination_gcp_project_id`   | String   | The GCP project id for the target dataset of restoring    |
| 2   | `destination_bigquery_dataset` | String   | The dataset name of the target dataset of restoring       |
| 3   | `source_gcs_archive`           | String   | The source GCS prefix which hosts the `dataset.json` file |

## Supported Bigquery Entities and their fields
1. Table
   1. project_id
   2. dataset
   3. description
   4. labels
   5. tags
   6. schema_fields
   7. data_archive_format
   8. data_compression
   9. partition_config
2. Partitioned Table (implemented by `Table` entity)
   1. project_id
   2. dataset
   3. description
   4. labels
   5. tags
   6. schema_fields
   7. data_archive_format
   8. data_compression
   9. partition_config 
3. View
   1. project_id
   2. dataset
   3. description
   4. labels
   5. tags
   6. schema_fields
   7. defining_query
4. Materialized View
   1. project_id
   2. dataset
   3. description
   4. labels
   5. tags
   6. schema_fields
   7. mview_query
   8. enable_refresh
   9. refresh_interval_seconds
   10. partition_config
5. Function
   1. project_id
   2. dataset
   3. description
   4. labels
   5. body
   6. arguments
   7. language
   8. return_type
6. Stored Procedure
   1. project_id
   2. dataset
   3. description
   4. labels
   5. body
   6. arguments
   7. language
   8. return_type

## Limitations
- The archive / restore leverage the user's GCP credentials to access the Bigquery and GCS resources. The user should have the necessary permissions to access the resources.