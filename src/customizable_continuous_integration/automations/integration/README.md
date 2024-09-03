# The python package to host customizable test framework

## Python Design
### Test case definition
A kind of test case is created as a python class, which is a subclass from the base class:
[BaseAutomationCommand](test_commands/base/base_command.py).
The test case class defines the actual test logic.

### Test case instantiation
A test case instance can be defined in the integration test config file by declaring the underlying test case class and 
related test config. This means the test case classes are reusable across various test case instances with different 
test configs associated.
An example of such config can be referred to:
[default integration config](/resources/config/integration_test.yaml)

## Config Design
The integration file is a well formatted YAML file with following top level fields defined:

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
| 4   | `test_args`       | Any        | A feild of arbitrary type used by test definition class as arguments                     |

## Available Test Classes
| No. | Test Name  | Description                                                                                                 |
|:----|:-----------|:------------------------------------------------------------------------------------------------------------|
| 1   | `dbt_test` | Run `dbt test` over a list of configured DBT projects pointed by the relative paths to this repository root |
