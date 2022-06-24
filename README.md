# Technical Subject Data Engineering Tiime

## Description

### Purpose

A pipeline to enrich transactions with tags & annotations.

### Stack

* Workflow: [metaflow](https://metaflow.org/)
* Data engineering: [pandas](https://pandas.pydata.org/)
* Data storage: [sqlite-utils](https://sqlite-utils.datasette.io/en/stable/) using [sqlite3](https://docs.python.org/3/library/sqlite3.html)
* Alerting (SMS): [twilio](https://www.twilio.com/docs/libraries/python)

## Installation & configuration

### Conda environment

Install the conda environment using the `environment.yml` file:
```
mamba env create -f environment.yml
```

An environment called `tiime` is created.

### Twilio configuration (not mandatory)

To enable SMS alerting in case of pipeline failure, you can specify twilio credentials in the `twilio_config.json` file.

Copy the example config file to the right path:
```
cp twilio_config.example.json twilio_config.json
```

Fill in the missing information in the `twilio_config.json` file using your preferred text editor:
- `account_sid`: your twilio account SID (available in the [twilio console](https://console.twilio.com/))
- `auth_token`: your twilio authentication token (available in the [twilio console](https://console.twilio.com/))
- `from_`: your twilio phone number (available in the [twilio console](https://console.twilio.com/))
- `to`: the phone number which will receive the alerting messages (e.g: your professional phone number)

## Usage

Activate the environment (do it once):
```
conda activate tiime
```

Show the pipeline doc (not mandatory):
```
python algorithm.py show
```

Run the pipeline:
```
python algorithm.py run
```

On the 1<sup>st</sup> pipeline run, a database file `database.db` is created in the root folder

## Backlog & improvements

### Log management

The builtin log management provided by [metaflow](https://metaflow.org/) is nice but:
- It does not rely on python `logging` system, only on `print` statements
  - All python modules using `logging` are not displayed in the logs. Nearly all the python modules don't use `print` to manage logs.
  - No way to have several levels of severity in the logs (`DEBUG` → `INFO` → `WARNING` → `ERROR` → `CRITICAL`)
- logs are scattered between various files:
  - on the local filesystem in the `.metaflow` folder, if the flow is run locally (which is the case here)
  - in [AWS CloudWatch](https://aws.amazon.com/cloudwatch/), if the flow is run using [AWS Batch](https://aws.amazon.com/batch/)

TODO:
- [ ] Find a way to use python `logging` system
- [ ] Find a way to push the logs in a centralized logging system (e.g: [datadog logs](https://docs.datadoghq.com/logs/))

### Failure management

Failure management properties of this implementation:
- the pipeline should always run successfully
- exceptions occuring during the run:
  - are handled using the `@catch` decorator 
  - are managed at the end of the pipeline: sending an SMS with the name of the step which failed
- if the pipeline fails, it means that an unexpected error occured

The implementation to do that is nice but it could be simpler. We need to write `if ... then ...` statements dedicated to failure management in the pipeline code. Ideally, this code should be:
- generic & implemented in a decorator or in [metaflow](https://metaflow.org/) pipeline management
- not part of the data engineering code.

TODO:
- [ ] Implement a [metaflow](https://metaflow.org/) decorator handling automatically the failures with SMS alerting, removing the need to write specific code.

### Data management

For the sake of the test, a local SQL database is used to store the results of the data pipeline. To be relative to using a remote database to store the data, instead of a local one. Since there will be a connection, Implement retrys using the `@retry` decorator

Also the [metaflow](https://metaflow.org/) API and this pipeline pattern (using a SQL data storage):
- does not make it clear what are the inputs and outputs of each step.
- does not allow easy rollback of the 

If it were to be done again, I would suggest trying 3 other patterns:
1. [S3](https://aws.amazon.com/s3/) buckets as a data warehouse:
   - store a new parquet dataset in [S3](https://aws.amazon.com/s3/) for each run, which could be deleted in case of issue or redundancy.
   - if needed, clean the redundant run dataset
   - if needed, have a pipeline building a dataset of all the runs. This step is not necessary if we use a parquet file reader able to read a collection of parquet files and emulate a database from that (e.g: [duckdb](https://duckdb.org/docs/data/parquet)).
2. [Google BigQuery](https://cloud.google.com/bigquery) as a data warehouse
3. [Amazon Redshift](https://docs.aws.amazon.com/redshift/index.html) backed by [S3](https://aws.amazon.com/s3/) buckets as a data warehouse

TODO:
- [ ] Use a remote database (e.g: [AWS RDS](https://aws.amazon.com/rds/))
- [ ] Implement proper credentials management
- [ ] Add `@retry` decorators to the steps having external connections

## Conclusion

The experiment shows that [metaflow](https://metaflow.org/) offers a nice set of features to manage pipeline documentation and execution.

However, the current pattern suffers several limitations:
- data is stored locally and in a centralized database, creating a single point of failure in the architecture
- logs are not managed in an industrial way (only `print` statements)
- failures are managed with specific code in the pipeline

Some of those limitations can be adressed, by improving the current pattern. However, I think that if given more time, I think I would try other patterns:
- Other workflow management tools, to improve log & failure management:
  - [luigi](https://luigi.readthedocs.io/en/stable/)
  - [kedro](https://kedro.readthedocs.io/en/stable/)
- Other data management tools, to improve data management:
  - [S3](https://aws.amazon.com/s3/) + [duckdb](https://duckdb.org/)
  - [BigQuery](https://cloud.google.com/bigquery)
  - [Redshift](https://docs.aws.amazon.com/redshift/index.html)


