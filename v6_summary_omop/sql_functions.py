import os

from v6_summary_omop.constants import *
from v6_summary_omop.utils import parse_sql_condition

def histogram(table, cohort_ids, arguments):
    """ Create the SQL statement to obtain the necessary information
        for an histogram.
    """
    width = None
    if BIN_WIDTH in arguments:
        width = arguments[BIN_WIDTH]
        if not isinstance(width, int) or width < int(os.getenv(BIN_WIDTH_MINIMUM) or BIN_WIDTH_MINIMUM_DEFAULT):
            raise Exception("Invalid bin width provided, value must be a integer superior to 1.")
    else:
        raise Exception("Histogram requested but the bin width (argument: BIN_WIDTH) must be provided!")

    return f"""SELECT floor("{VALUE}"/{width})*{width} as bins, COUNT(*) 
        FROM ({table}) AS t {parse_sql_condition(cohort_ids, where_condition=True)} 
        GROUP BY 1 ORDER BY 1;"""

def quartiles(table, cohort_ids, arguments):
    """ Create the SQL statement to obtain the 25th, 50th, and 75th 
        quartiles for a variable.
    """
    iqr_threshold = arguments.get(IQR_THRESHOLD) or IQR_THRESHOLD_DEFAULT
    return f"""with percentiles AS (SELECT current_database() as db,
        percentile_cont(0.25) within group (order by "{VALUE}" asc) as q1,
        percentile_cont(0.50) within group (order by "{VALUE}" asc) as q2,
        percentile_cont(0.75) within group (order by "{VALUE}" asc) as q3
        FROM ({table}) AS t 
        {parse_sql_condition(cohort_ids, where_condition=True)}) 
        SELECT *, q1 - (q3 - q1) * {iqr_threshold} AS lower_bound,
        q3 + (q3 - q1) * {iqr_threshold} AS upper_bound, 
        (SELECT count("{VALUE}") FROM ({table}) AS t WHERE 
            "{VALUE}" < q1 - (q3 - q1) * {iqr_threshold} 
            {parse_sql_condition(cohort_ids)}) AS lower_outliers, 
        (SELECT count("{VALUE}") FROM ({table}) AS t WHERE 
            "{VALUE}" > q3 + (q3 - q1) * {iqr_threshold} 
            {parse_sql_condition(cohort_ids)}) AS upper_outliers
        FROM percentiles;
    """

def count_null(table, cohort_ids, arguments):
    """ Create the SQL statment to count the null values.
    """
    return f"""SELECT count("{VALUE}") FROM ({table}) AS t 
        WHERE "{VALUE}" IS NULL {parse_sql_condition(cohort_ids)};"""

def count_discrete_values(table, cohort_ids, arguments):
    """ Count the discrete values.
    """
    return f"""SELECT "{VALUE}", count(*) FROM ({table}) AS t 
        {parse_sql_condition(cohort_ids, where_condition=True)} 
        GROUP BY "{VALUE}";"""
