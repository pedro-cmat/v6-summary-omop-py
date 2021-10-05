""" Utility functions.
"""
import os

from v6_summary_omop.constants import *

def run_sql(db_client, sql_statement, parameters=None, fetch_all=False):
    """ Execute the sql query and retrieve the results
    """
    db_client.execute(sql_statement, parameters)
    if fetch_all:
        return db_client.fetchall()
    else:
        return db_client.fetchone()

def parse_error(error_message, error=None):
    """ Parse an error message.
    """
    return {
        MESSAGE: error_message,
        ERROR: error 
    }

def check_keys_in_dict(keys, map):
    """ Check if all keys are present in a dictionary.
    """
    return all([key in map for key in keys])

def compare_with_minimum(value):
    """ Compare the value with the minimum value allowed.
    """
    count_minimum = int(os.getenv(COUNT_MINIMUM) or COUNT_MINIMUM_DEFAULT)
    return value if value > count_minimum else f"< {count_minimum}"

def parse_sql_condition(cohort_ids, where_condition=False):
    """ Parse the sql condition to insert in another sql statement.
    """
    return f"""{"WHERE" if where_condition else "AND"} id IN {cohort_ids}""" \
        if cohort_ids else ""
