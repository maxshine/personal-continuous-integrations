CREATE SCHEMA IF NOT EXISTS `{{ project_id }}.{{ dataset_name }}` OPTIONS (description="archive dev dataset", labels=[("label0","value0")]);

-- tables
CREATE TABLE `pers-decision-engine-dev.ygao_bigquery_archive_dev.partition_table`
(
  id INT64,
  name STRING,
  partition_date DATE
)
PARTITION BY partition_date
OPTIONS(
  description="a partition table",
  labels=[("label2", "value2")]
);

CREATE EXTERNAL TABLE `pers-decision-engine-dev.ygao_bigquery_archive_dev.external_spreadsheet_table`
(
  id INT64,
  name STRING,
  salary NUMERIC
)
OPTIONS(
  description="external spreadsheet table",
  labels=[("label2", "value2")],
  sheet_range="",
  format="GOOGLE_SHEETS",
  uris=["https://docs.google.com/spreadsheets/d/1cIzh84v-H1UQ0uCf5M-RAcLpfiI1OmrvgstVud9Hq6g"]
);

CREATE TABLE `pers-decision-engine-dev.ygao_bigquery_archive_dev.range_partition_table`
(
  customer_id INT64,
  name STRING
)
PARTITION BY RANGE_BUCKET(customer_id, GENERATE_ARRAY(0, 100, 10));

CREATE TABLE `pers-decision-engine-dev.ygao_bigquery_archive_dev.general_table`
(
  id INT64,
  name STRING,
  sample_datetime DATETIME,
  sample_timestamp TIMESTAMP
)
OPTIONS(
  description="a general table",
  labels=[("label1", "value1")]
);

CREATE EXTERNAL TABLE `pers-decision-engine-dev.ygao_bigquery_archive_dev.external_gcs_table`
OPTIONS(
  description="external gcs table",
  labels=[("label1", "value1")],
  format="AVRO",
  uris=["gs://pde-dev-tt/tt-dev/ygao_test/archive_test/project=pers-decision-engine-dev/dataset=ygao_bigquery_archive_dev/archive_ts=20250409234929/tables/table=general_table/data/*"]
);

-- functions
CREATE FUNCTION `pers-decision-engine-dev`.ygao_bigquery_archive_dev.js_function(tags JSON) RETURNS STRUCT<pillar_id STRING, campaign_code STRING, campaign_type STRING, campaign_category STRING> LANGUAGE js
OPTIONS(
  description="A general js UDF")
AS
r"""
function convertListToDict(list) {
    let result = {};
    const keyMap = {
        'pde': 'pillar_id',
        'campaign_code': 'campaign_code',
        'campaign_type': 'campaign_type',
        'campaign_category': 'campaign_category'
    };
    list.forEach(item => {
        let [key, value] = item.split(':');
        if (keyMap[key]) {
            result[keyMap[key]] = value;
        }
    });
    return result;
}
return convertListToDict(tags)
""";

CREATE PROCEDURE `pers-decision-engine-dev`.ygao_bigquery_archive_dev.create_customer()
OPTIONS(
  description="general stored procedure")
BEGIN
  DECLARE name STRING;
  SET name = GENERATE_UUID();
  INSERT INTO ygao_bigquery_archive_dev.general_table (id, name)
    VALUES(123, name);
  SELECT FORMAT("Created customer %s", name);
END;

CREATE FUNCTION `pers-decision-engine-dev`.ygao_bigquery_archive_dev.AddFourAndDivide(x INT64, y INT64) RETURNS FLOAT64
OPTIONS(
  description="general function")
AS (
(x + 4) / y
);

-- views
CREATE VIEW `pers-decision-engine-dev.ygao_bigquery_archive_dev.general_view`
OPTIONS(
  description="a general view",
  labels=[("label3", "value3")]
)
AS select 3 as id, "ccc" as name;

CREATE MATERIALIZED VIEW `pers-decision-engine-dev.ygao_bigquery_archive_dev.materialized_view`
OPTIONS(
  description="A archive materialized view",
  labels=[("label4", "value4")]
)
AS select * from pers-decision-engine-dev.PDE_DATA_OPS.SEED_CARDS;

CREATE VIEW `{{ project_id }}.{{ dataset_name }}.referencing_view`
OPTIONS(
  description="a referencing view",
  labels=[("label5", "value5")]
)
AS select * except(sample_datetime, sample_timestamp) from pers-decision-engine-dev.ygao_bigquery_archive_dev.general_table
  union all
  select * except(partition_date) from ygao_bigquery_archive_dev.partition_table
  union all
  select * from ygao_bigquery_archive_dev.general_view;

CREATE VIEW `pers-decision-engine-dev.ygao_bigquery_archive_dev.another_referencing_view`
OPTIONS(
  description="another referencing view to read data from external project and dataset",
  labels=[("label1", "value1")]
)
AS select * from pers-decision-engine-test.PDE_DATA_OPS.SEED_CARDS;

CREATE VIEW `pers-decision-engine-dev.ygao_bigquery_archive_dev.udf_view`
AS select
  `pers-decision-engine-dev.ygao_bigquery_archive_dev`.js_function(JSON_ARRAY("pde:pillar_id", "campaign_code:abc-123")).pillar_id as id,
  `ygao_bigquery_archive_dev`.js_function(JSON_ARRAY("pde:pillar_id", "campaign_code:abc-123")).campaign_code as code;