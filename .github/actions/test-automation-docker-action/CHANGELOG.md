# Change History of [test-automation-docker-action](action.yml)

## Versions
### v1.3.0
- Features
  1. Initial version of test command running `dbt test`
- Bugfix
N/A
- Dependencies
  1. DBT Core version 1.8.5 with Bigquery adapter 1.8.2

### v1.3.1 (bugfix release succeeding from v1.3.0)
- Features
N/A
- Bugfix
N/A
- Dependencies
  1. DBT Core version 1.8.5 with Bigquery adapter 1.8.2

### v1.3.2 (bugfix release succeeding from v1.3.1)
- Features
N/A
- Bugfix
N/A
- Dependencies
  1. DBT Core version 1.8.5 with Bigquery adapter 1.8.2

### v1.3.3 (feature release succeeding from v1.3.2)
- Features
1. Support DBT configs from test config file for `dbt_test` command
- Bugfix
N/A
- Dependencies
  1. DBT Core version 1.8.5 with Bigquery adapter 1.8.2

### v1.3.4 (feature release succeeding from v1.3.3)
- Features
1. Support DBT profiles in the config from test config file for `dbt_test` command 
2. Support running models before testing with the config field `build_before_test`
- Bugfix
N/A
- Dependencies
  1. DBT Core version 1.8.5 with Bigquery adapter 1.8.2

### v1.3.5 (collateral release succeeding from v1.3.4)
- Features
N/A
- Bugfix
N/A

### v1.4.1 (collateral release succeeding from v1.3.4 to latest codebase)
- Features
N/A
- Bugfix
N/A

### v1.4.2 (collateral release succeeding from v1.4.1 to latest codebase)
- Features
N/A
- Bugfix
N/A

### v1.4.3 (feature release succeeding from v1.4.2)
- Features
1. Upgrade to Python 3.12
2. Upgrade to DBT Core 1.10.4 with Bigquery adapter 1.10.2
- Bugfix
N/A

### v1.4.4 (feature & bugfix release succeeding from v1.4.3)
- Features
1. Use [uv](https://github.com/astral-sh/uv) as package installer to speed up runtime building up.
- Bugfix
1. When multiple tests defined in the row, make sure all tests are executed from the same working directory.
