# log4j2-appenders
This is a repository for a set of useful [Log4j 2][log4j2] appenders.

# Contributing
Follow the steps below to develop and contribute to the project.

## Set up
Initialize and update the submodules:
```shell
git submodule update --init --recursive
```

## Requirements

* Java 11
* Maven 3.8 or newer

## Building
Build and test:
```shell
mvn package
```

Build without testing:
```shell
mvn package -DskipTests
```

## Testing
```shell
mvn test
```

## Linting
Before submitting a pull request, ensure you’ve run the linting [commands](#running-the-linters)
below and either fixed any violations or suppressed the warnings.

### Requirements
* Python 3.10 or higher
* [Task] 3.38.0 or higher

### Adding files
Certain file types need to be added to our linting rules manually:

* If adding a **YAML** file (regardless of its extension), add it as an argument to the `yamllint`
  command in [lint-tasks.yaml](lint-tasks.yaml).

### Running the linters
To run all linting checks:
```shell
task lint:check
```

To run all linting checks AND automatically fix any fixable issues:
```shell
task lint:fix
```

[log4j2]: https://logging.apache.org/log4j/2.x/index.html
[Task]: https://taskfile.dev
