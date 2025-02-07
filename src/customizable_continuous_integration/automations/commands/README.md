# The python package to host user-interface CLI

## Available Commands
| No. | Field              | Starting Version | Description                                               |
|:----|:-------------------|:-----------------|:----------------------------------------------------------|
| 1   | `integration-test` | v1.0.0           | Run the integration test suite defined in the config file |
| 2   | `run-shell`        | v1.0.0           | Run arbitrary Unix command in the Bash shell              |
| 3   | `write-protection` | v1.0.0           | Check the protected files in a GIT difference             |


## Commands Release History
### integration-test
Available from **v1.3.0**.   
Deprecated from *N/A*.
#### Version History
| No. | Version | Features                                                                                                                         | Bugfixes |
|:----|:--------|:---------------------------------------------------------------------------------------------------------------------------------|:---------|
| 1   | v1.3.0  | - Initial version of test command running `dbt test`                                                                             | N/A      |
| 2   | v1.3.3  | - Support DBT configs in the config for `dbt_test` command                                                                       | N/A      |
| 3   | v1.3.4  | - Support DBT profiles in the config for `dbt_test` command<br> - Support running models before testing with `build_before_test` | N/A      |


### run-shell
Available from **v1.0.0**.   
Deprecated from *N/A*.
#### Version History
| No. | Version | Features                         | Bugfixes |
|:----|:--------|:---------------------------------|:---------|
| 1   | v1.0.0  | - Run an arbitrary shell command | N/A      |

### write-protection
Available from **v1.0.0**.   
Deprecated from *N/A*.
#### Version History
| No. | Version | Features                                                                             | Bugfixes                                                |
|:----|:--------|:-------------------------------------------------------------------------------------|:--------------------------------------------------------|
| 1   | v1.0.0  | - Check changed files in PR<br>- Support include / exclude filter to scope files<br> | N/A                                                     |
| 2   | v1.1.0  | - Support checking across forked repositories with `forked-repository-url`           | N/A                                                     |
| 3   | v1.2.0  | - Support administrative users config via collaborator roles (admin and above)       | N/A                                                     |
| 4   | v1.2.1  | N/A                                                                                  | - Use maintainer role instead of admin                  |
| 5   | v1.3.1  | N/A                                                                                  | - Use maintainer role instead of admin (based on 1.3.0) |
