# log4j2-appenders
This is a repository for a set of useful [Log4j 2][log4j2] appenders. Currently, it contains:

* `ClpIrFileAppender` - Used to compress log events using [CLP](https://github.com/y-scope/clp)'s IR
  stream format, allowing users to achieve higher compression than general-purpose compressors while
  the logs are being generated.

* `AbstractBufferedRollingFileAppender` - An abstract class which enforces an opinionated workflow,
  skeleton interfaces, and hooks optimized towards buffered rolling file appender implementations
  with remote persistent storage. In addition, the abstract class implements a log-level-aware
  hard + soft timeout-based log-freshness policy.

* `AbstractClpirBufferedRollingFileAppender` - Provides size-based file rollover, log freshness
  guarantee, and streaming compression offered by `ClpIrFileAppender`.

# Usage
## `ClpIrFileAppender`
1. Add the package and its dependencies to the `dependencies` section of your `pom.xml`:

   ```xml
   <dependencies>
     <!-- The appenders -->
     <dependency>
       <groupId>com.yscope.logging</groupId>
       <artifactId>log4j2-appenders</artifactId>
       <version>0.1.0</version>
     </dependency>

     <!-- Packages that log4j2-appenders depends on -->
     <dependency>
       <groupId>com.github.luben</groupId>
       <artifactId>zstd-jni</artifactId>
       <version>1.5.6-7</version>
     </dependency>
     <dependency>
       <groupId>org.apache.logging.log4j</groupId>
       <artifactId>log4j-api</artifactId>
       <version>2.24.1</version>
     </dependency>
     <dependency>
       <groupId>org.apache.logging.log4j</groupId>
       <artifactId>log4j-core</artifactId>
       <version>2.24.1</version>
     </dependency>
   </dependencies>
   ```

2. Add the appender to your Log4j2 configuration file. Here is a sample `log4j2.properties` file:

   ```properties
    rootLogger.level = INFO
    rootLogger.appenderRef.clpir.ref = clpir

    appender.clpir.type = com.yscope.logging.log4j2.ClpIrFileAppender
    appender.clpir.layout.type = PatternLayout

    # NOTE:
    # 1. This appender doesn't require a date conversion pattern in the conversion pattern. This is
    #    because the CLP appender stores the timestamp separately from the message. CLP's IR
    #    decoders will allow users to specify their desired timestamp format when decoding the logs.
    # 2. If a date conversion pattern is added, it will be removed from the conversion pattern. This
    #    may result in an ugly conversion pattern since the spaces around the date pattern are not
    #    removed.
    appender.clpir.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
    appender.clpir.file=logs.clp.zst

    # Use CLP's four-byte encoding for lower memory usage at the cost of some compression ratio
    appender.clpir.UseFourByteEncoding = true

    # closeFrameOnFlush:
    # - true: Any data buffered by the compressor is immediately flushed to disk; frequent flushes
    #   may lower compression ratio significantly
    # - false: Any compressed data that is ready for writing will be flushed to disk
    appender.clpir.CloseFrameOnFlush=false

    # compressionLevel: Higher compression levels may increase compression ratio but will slow down
    # compression. Valid compression levels are 1-19.
    appender.clpir.CompressionLevel = 3
   ```

## `AbstractClpIrBufferedRollingFileAppender`
To use this appender, at minimum, we expect the user to implement the `sync()` method to perform
file upload to remote persistent storage.

# Providing Feedback
You can use GitHub issues to [report a bug][report-bug] or [request a feature][feature-req].

# Contributing
Follow the steps below to develop and contribute to the project.

## Set up
Initialize and update the submodules:
```shell
git submodule update --init --recursive
```

## Requirements

* JDK 11
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

### Running specific linters
The commands above run all linting checks, but for performance you may want to run a subset (e.g.,
if you only changed Java files, you don't need to run the YAML linting checks) using one of the
tasks in the table below.

| Task                     | Description                                               |
|--------------------------|-----------------------------------------------------------|
| `lint:java-check`        | Runs the Java linters (formatters and static analyzers).  |
| `lint:java-fix`          | Runs the Java linters and fixes some violations.          |
| `lint:java-format-check` | Runs the Java formatters.                                 |
| `lint:java-format-fix`   | Runs the Java formatters and fixes some violations.       |
| `lint:java-static-check` | Runs the Java static analyzers.                           |
| `lint:java-static-fix`   | Runs the Java static analyzers and fixes some violations. |
| `lint:yml-check`         | Runs the YAML linters.                                    |
| `lint:yml-fix`           | Runs the YAML linters and fixes some violations.          |

[feature-req]: https://github.com/y-scope/log4j2-appenders/issues/new?assignees=&labels=enhancement&template=feature-request.yml 
[log4j2]: https://logging.apache.org/log4j/2.x/index.html
[report-bug]: https://github.com/y-scope/log4j2-appenders/issues/new?assignees=&labels=bug&template=bug-report.yml
[Task]: https://taskfile.dev
