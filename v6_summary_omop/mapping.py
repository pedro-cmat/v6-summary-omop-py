from v6_summary_omop.aggregators import *
from v6_summary_omop.constants import *
from v6_summary_omop.sql_functions import *

AGGREGATORS = {
    MAX_FUNCTION: maximum,
    MIN_FUNCTION: minimum,
    AVG_FUNCTION: average,
    POOLED_STD_FUNCTION: pooled_std,
    HISTOGRAM: histogram_aggregator,
    BOXPLOT: boxplot,
    COUNT_FUNCTION: count,
    COUNT_NULL: sum_null,
    COUNT_DISCRETE: count_discrete,
}

FUNCTION_MAPPING = {
    COUNT_FUNCTION: {
        FUNCTIONS: [COUNT_FUNCTION]
    },
    AVG_FUNCTION: {
        FUNCTIONS: [SUM_FUNCTION, COUNT_FUNCTION]
    },
    MAX_FUNCTION: {
        FUNCTIONS: [MAX_FUNCTION]
    },
    MIN_FUNCTION: {
        FUNCTIONS: [MIN_FUNCTION]
    },
    SUM_FUNCTION: {
        FUNCTIONS: [SUM_FUNCTION]
    },
    POOLED_STD_FUNCTION: {
        FUNCTIONS: [STD_SAMP_FUNCTION]
    },
    HISTOGRAM: {
        METHOD: {
            NAME: HISTOGRAM,
            CALL: histogram,
            FETCH: FETCH_ALL
        },
    },
    BOXPLOT: {
        FUNCTIONS: [MAX_FUNCTION, MIN_FUNCTION],
        METHOD: {
            NAME: QUARTILES,
            CALL: quartiles,
            FETCH: FETCH_ONE
        }
    },
    COUNT_NULL: {
        METHOD: {
            NAME: COUNT_NULL,
            CALL: count_null,
            FETCH: FETCH_ONE
        }
    },
    COUNT_DISCRETE: {
        METHOD: {
            NAME: COUNT_DISCRETE,
            CALL: count_discrete_values,
            FETCH: FETCH_ALL
        }
    }
}
