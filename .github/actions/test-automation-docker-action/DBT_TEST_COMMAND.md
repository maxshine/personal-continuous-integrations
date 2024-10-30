## This is a test command to run `dbt test` against configured DBT projects

### Features
1. Run a bunch of DBT projects if they can be tested with the same configuration
2. Support the setup of DBT variables / sources / dependencies

#### Test config element Schema
| No. | Config Field    | Type           | Description                                                                               |
|:----|:----------------|:---------------|:------------------------------------------------------------------------------------------|
| 1   | test_projects   | List of String | A list of DBT project path relative to the repository root                                |
| 2   | dbt_source      | Dictionary     | A mapping hold the same structure as DBT sources. Content will be put into `sources.yml`  |
| 3   | dbt_variable    | Dictionary     | A mapping hold the same structure as DBT vars. Content will be put into `dbt_project.yml` |
| 4   | dbt_dependency  | Dictionary     | A mapping hold the same structure as DBT sources. Content will be put into `package.yml`  |

#### Supported DB vendor adapters by default
1. BigQuery (since v1.4.0)