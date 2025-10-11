# The python package to host customizable test framework

## Detailed Change History
[Change History of integration-test action](/.github/actions/test-automation-docker-action/CHANGELOG.md)

## Python Design
### Test case definition
A kind of test case is created as a python class, which is a subclass from the base class:
[BaseAutomationCommand](test_commands/base/base_command.py).
The test case class defines the actual test logic.

### Test case instantiation
A test case instance can be defined in the integration test config file by declaring the underlying test case class and 
related test config. This means the test case classes are reusable across various test case instances with different 
test configs associated.
An example of such a config can be referred to:
[default integration config](/resources/config/integration_test.yaml)

## Config Design
The integration file is a well-formatted YAML file with the following top level fields defined:

| No. | Field         | Type       | Description                                                                  |
|:----|:--------------|:-----------|:-----------------------------------------------------------------------------|
| 1   | `concurrency` | Integer    | A future enhancement will used it to switch to the concurrent execution      |
| 2   | `tests`       | Dictionary | The key is a test instance name and the value will be the test relevant data |

Test entry value schema

| No. | Field             | Type       | Description                                                                              |
|:----|:------------------|:-----------|:-----------------------------------------------------------------------------------------|
| 1   | `command`         | String     | A pre-defined value for users to access the test definition class                        |
| 2   | `test_config`     | Dictionary | An mapping hosting test config, which is subject to the control of test definition class |
| 3   | `throw_exception` | Boolean    | Whether let the test fail at any uncaught exceptions                                     |
| 4   | `test_args`       | Any        | A field of arbitrary type used by test definition class as arguments                     |

## Available Test Classes
| No. | Test Name    | Description                                                                                                                                                       |
|:----|:-------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1   | `dbt_test`   | Run DBT `TEST` command over a list of configured DBT projects. Refer to [DBT_TEST_COMMAND.md](/.github/actions/test-automation-docker-action/DBT_TEST_COMMAND.md) |
| 2   | `dbt_action` | Run a general DBT command over a list of configured DBT projects.                                                                                                 |
