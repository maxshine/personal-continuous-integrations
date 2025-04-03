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
N/A

### v1.4.1
- Features
  1. Add `archiver_version` field to the archive for future compatibility.
     - This field is used to identify the version of the archiver that created the archive.
     - This field is useful for future compatibility and can be used to determine if the archive can be restored by the current version of the archiver.
  2. Set `DEFLATE` compression for the archive for better storage efficiency.

- Bugfix
N/A