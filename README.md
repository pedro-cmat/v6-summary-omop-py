# Federated RDB Summary

Algorithm based on the Vantage 6 Federated Summary for relational databases following the OMOP CDM v6.0.
It reports the following information from each node:
- `Min`
- `Max`
- `Mean`
- `Pooled Standard Deviation`
- `Count`
- `Histogram`
- `Boxplot` (reported individually for each node)

Additionally, it's also possible to evaluate the number of participants for a cohort.

The data privacy can be enhanced using environment variables that restrict the access to
datasets that have a minimum amount of records. This is also applied to the results obtained 
from the different functions provided. See more below.

## Node Setup

Make sure to set the database connection parameters as environment variables using the default variables for a postgres database (https://www.postgresql.org/docs/9.3/libpq-envars.html):

```yaml
    application:
        ...
        algorithm_env:
            PGUSER: <user>
            PGPASSWORD: <password>
            PGDATABASE: <database>
            PGPORT: <port>
            PGHOST: <host>
```

## Usage
```python
from vantage6.client import Client

# Create, athenticate and setup client
client = Client("http://127.0.0.1", 5000, "")
client.authenticate("researcher@center.nl", "password")
client.setup_encryption(None)

# Define algorithm input
# The summary functions to be computed for each column will be selected in the following order:
# 1. the functions provided for a specific column
# 2. the functions provided for all columns
# 3. all functions will be computed
input_ = {
    "master": "true",
    "method":"master", 
    "args": [], 
    "kwargs": {
        "functions": [],
        "columns": [
            {
                "variable": "year_of_birth",
                "table": "PERSON",
                "functions": ["min", "max", "avg"]
            },
            {
                "variable": "381316",
                "table": "CONDITION_OCCURRENCE"
                "functions": ["count_null", "count", "count_discrete"],
            }
        ],
        # Optional argument to specify the organizations
        # "org_ids": [2, 3]
    }
}

# Send the task to the central server
task = client.post_task(
    name="summary",
    image="pmateus/v6-summary-omop:1.2.0",
    collaboration_id=1,
    input_= input_,
    organization_ids=[2]
)

# Retrieve the results
res = client.get_results(task_id=task.get("id"))
```

### Histogram

The histogram function requires the bin width to be provided using the following variable `BIN_WIDTH`.

```python
input_ = {
    "master": "true",
    "method":"master", 
    "args": [], 
    "kwargs": {
        "functions": [],
        "columns": [
            {
                "variable": "age",
                "table": "records",
                "functions": ["histogram"],
                "BIN_WIDTH": 4
            }
        ]
    }
}
```

### Boxplot

The Boxplot function allows to specify the IQR used to determine the boundaries.
By default this value is 1.5 but it can be changed using the following variable `IQR_THRESHOLD`.

```python
input_ = {
    "master": "true",
    "method":"master", 
    "args": [], 
    "kwargs": {
        "functions": [],
        "columns": [
            {
                "variable": "age",
                "table": "records",
                "functions": ["boxplot"],
                "IQR_THRESHOLD": 2
            }
        ]
    }
}
```

### Cohort

The cohort function allows to explore possible groups of participants based on a set of 
characteristics that can be set using the SQL operators:

```python
input_ = {
    "master": "true",
    "method":"master", 
    "args": [], 
    "kwargs": {
        "cohort": {
            "definition": [
                {
                    "variable": "Age",
                    "operator": ">=",
                    "value": 75
                },
                {
                    "variable": "deadstatus.event",
                    "operator": "=",
                    "value": 1
                },
                {
                    "variable": "Histology",
                    "operator": "IN",
                    "value": "('large_cell', 'scc')"
                }
            ],
            "table": "records",
            "id_column": "ID"
        }
    }
}
```

Combined with the `columns` argument, it's possible to obtain summary statistics 
regarding the selected cohort:

```python
input_ = {
    "master": "true",
    "method":"master", 
    "args": [], 
    "kwargs": {
        "cohort": {
            "definition": [
                {
                    "variable": "deadstatus.event",
                    "operator": "=",
                    "value": 1
                }
            ],
            "table": "records",
            "id_column": "ID"
        },
        "columns": [
            {
                "variable": "age",
                "table": "records",
                "functions": ["avg"]
            }
        ]
    }
}
```

In this example, we would be calculating the age average for the group of 
participants that has 1 as the value for the dead status.

## Privacy

Federated learning distinguishes itself by bringing additional security and privacy 
by keeping the data at each center.
Keeping that in mind, the following variables give control over the algorithm:
- `TABLE_MINIMUM` (default value = 10): minimum number of records available in the 
requested table to run the functions or the cohort functionality;
- `COUNT_MINIMUM` (default value = 5): minimum value allowed to be presented in the 
results when counting the number of records/outliers/...;
- `BIN_WIDTH_MINIMUM` (default value = 2): the minimum width for the histogram bins;

These environment variables can be used in the `algorithm_env` field when configuring 
the vantage6 node.

## Test / Develop

You need to have Docker installed.

To Build (assuming you are in the project-directory):
```
docker build -t v6-summary-rdb .
```
