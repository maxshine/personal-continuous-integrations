## This is a test command to run `dbt test` against configured DBT projects

### Features
1. Run a bunch of DBT projects if they can be tested with the same configuration
2. Support the setup of DBT variables / sources / dependencies

#### Test config element Schema
| No. | Config Field      | Type           | Description                                                                                |
|:----|:------------------|:---------------|:-------------------------------------------------------------------------------------------|
| 1   | target_projects   | List of String | A list of DBT project path relative to the repository root                                 |
| 2   | dbt_source        | Dictionary     | A mapping hold the same structure as DBT sources. Content will be put into `sources.yml`   |
| 3   | dbt_variable      | Dictionary     | A mapping hold the same structure as DBT vars. Content will be put into `dbt_project.yml`  |
| 4   | dbt_dependency    | Dictionary     | A mapping hold the same structure as DBT packages. Content will be put into `package.yml`  |
| 5   | dbt_profile       | Dictionary     | A mapping hold the same structure as DBT profiles. Content will be put into `profiles.yml` |
| 6   | build_before_test | Boolean        | A flag to control whether run models before executing any test cases                       |

#### Supported DB vendor adapters by default
1. BigQuery (since v1.3.0)