## This is an action to run configured test automations

### Features
1. A integration test framework supports declarative test cases plus corresponding test configs
2. The action can run the declared test cases based a single input config file

### Synopsis in Workflow
TBD

### Test Commands
| No. | Test Command | Description                                                                                         |
|:----|:-------------|:----------------------------------------------------------------------------------------------------|
| 1   | dbt_test     | Check [the DBT Test Command README](DBT_TEST_COMMAND.md) |

### Config Schema
| No. | Test Command              | Type       | Description                                                                                          |
|:----|:--------------------------|:-----------|:-----------------------------------------------------------------------------------------------------|
| 1   | concurrency               | Integer    | The amount of workers to run defined cases concurrently                                              |
| 2   | tests                     | Dictionary | A dictionary holds test cases. Each element is keyed with test case name and the value is the config |
| 3   | continue_on_failure       | Boolean    | Switch whether the automation exit on failure                                                        |

#### Test config element Schema
| No. | Config Field    | Type       | Description                                                                                      |
|:----|:----------------|:-----------|:-------------------------------------------------------------------------------------------------|
| 1   | command         | String     | The command to run this test case                                                                |
| 2   | test_config     | Dictionary | A dictionary holds the config for the test command. Check command documents for specific schema. |
| 3   | throw_exception | Boolean    | Switch whether command should throw the errors                                                   |
| 4   | test_args       | Boolean    | Complimentary arguments to run the test cases                                                    |


### Inputs
1. `test-config-file`: Required. The local path to the test config file in the tested repository 

### Outputs
N/A

### Availability
Available from **v1.3.0**.   
Deprecated from *N/A*  
Please refer to the [Changelog](CHANGELOG.md) for details

### License

**Protect File Change By Pull Request's** license is included here:

```
The MIT License (MIT)

Copyright (c) 2024 Ryan Gao

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
```