PGDATABASE = "PGDATABASE"

COUNT_FUNCTION = "count"
AVG_FUNCTION = "avg"
MAX_FUNCTION = "max"
MIN_FUNCTION = "min"
SUM_FUNCTION = "sum"
STD_SAMP_FUNCTION = "stddev_samp"
POOLED_STD_FUNCTION = "pooled_std"
COUNT_NULL = "count_null"
COUNT_DISCRETE = "count_discrete"

HISTOGRAM = "histogram"
BOXPLOT = "boxplot"
QUARTILES = "quartiles"

VARIABLE = "variable"
TABLE = "table"
FUNCTIONS = "functions"
REQUIRED_FUNCTIONS = "required_functions"
REQUIRED_METHODS = "required_methods"
METHOD = "METHOD"
CONCEPT_ID = "concept_id"

ERROR = "error"
WARNING = "warning"
MESSAGE = "MESSAGE"

NAME = "NAME"
CALL = "CALL"
FETCH = "FETCH"
FETCH_ONE = "FETCH_ONE"
FETCH_ALL = "FETCH_ALL"

TABLE_MINIMUM = "TABLE_MINIMUM"
TABLE_MINIMUM_DEFAULT = 10
COUNT_MINIMUM = "COUNT_MINIMUM"
COUNT_MINIMUM_DEFAULT = 5
BIN_WIDTH_MINIMUM = "BIN_WIDTH_MINIMUM"
BIN_WIDTH_MINIMUM_DEFAULT = 2
IQR_THRESHOLD_DEFAULT = 1.5

BIN_WIDTH = "BIN_WIDTH"
IQR_THRESHOLD = "IQR_THRESHOLD"
COHORT = "COHORT"
COHORT_DEFINITION = "definition"
OPERATOR = "operator"
VALUE = "value"

PERSON_ID = "person_id"
PERSON_TABLE = "person"
OBSERVATION_TABLE = "observation"
OBSERVATION_CONCEPT_ID = "observation_concept_id"
MEASUREMENT_TABLE = "measurement"
MEASUREMENT_CONCEPT_ID = "measurement_concept_id"
CONDITION_TABLE = "condition_occurrence"
CONDITION_CONCEPT_ID = "condition_concept_id"

VALUE_AS_STRING = "value_as_string"
VALUE_AS_CONCEPT_ID = "value_as_concept_id"

DEFAULT_FUNCTIONS = [
    MAX_FUNCTION, MIN_FUNCTION, AVG_FUNCTION, POOLED_STD_FUNCTION
]
