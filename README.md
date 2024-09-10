# The python project to host sharable continuous interation automations

## Project structure
| No. | Paths            | Description                                                            |
|:----|:-----------------|:-----------------------------------------------------------------------|
| 1   | `resources`      | The directory for resource files, e.g. default integration test config |
| 2   | `src`            | The directory for python source files                                  |
| 3   | `tests`          | The directory for python test files                                    |
| 4   | `pyproject.toml` | The python project definition file                                     |
| 5   | `README.md`      | The instruction and guidance file for this CI project                  |


## Available pre-commit hooks
| No. | Hook                       | Description                                      |
|:----|:---------------------------|:-------------------------------------------------|
| 1   | github-write-protection-pr | Check [the description](/.pre-commit-hooks.yaml) |


## Available Github sharable actions
| No. | Action                         | Description                                                                   |
|:----|:-------------------------------|:------------------------------------------------------------------------------|
| 1   | hello-world-docker-action      | Check [the README](/.github/actions/hello-world-docker-action/README.md)      |
| 2   | write-protection-docker-action | Check [the README](/.github/actions/write-protection-docker-action/README.md) |