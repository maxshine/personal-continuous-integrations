# Change History of [bigquery-archiver](README.md)

## Versions
### v1.4.0
- Features
  1. archive a Bigquery dataset into GCS and restore it from the archive to any arbitrary dataset.
  2. table archives are compressed
  3. Support the archive and restore of the following entities (refer to [the README](README.md) for more details):
     1. table 
     2. partitioned table
     3. view
     4. materialized view
     5. function
     6. stored procedure
- Bugfix
N/A

### v1.4.1
- Features
  1. Add `archiver_version` field to the archive for future compatibility.
     - This field is used to identify the version of the archiver that created the archive.
     - This field is useful for future compatibility and can be used to determine if the archive can be restored by the current version of the archiver.
  2. Set `DEFLATE` compression for the archive for better storage efficiency.
- Bugfix
N/A

### v1.4.2
- Features
  1. Add `attach_archive_ts_to_label` field to the archive for future compatibility.
     - This field is used to attach the archive timestamp to the label of the archive.
     - This field is defaulted to `true` and can be set to `false` to disable the attachment.
  2. Add `description` field in restored routine entities.
  3. Support external table entities in the process.
  4. Add checks over necessary config fields and exit with error if any missed.
- Bugfix
  1. Strip tailing slash from the GCS path in the process.

### v1.4.3
- Features
  1. Workaround the issue of restoring AVRO datetime fields. Reference: https://cloud.google.com/bigquery/docs/exporting-data#avro_export_details
  2. Support JavaScript UDFs in the process.
  3. Add a DDL script to generate a development dataset for testing purposes.
- Bugfix
  1. Fix the issue of routine references missing replacements in the view restoration process.

### v1.4.4 (collateral release from v1.4.3 to latest codebase)
- Features
N/A
- Bugfix
N/A