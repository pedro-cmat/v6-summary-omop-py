import os

from v6_summary_omop.constants import *
from v6_summary_omop.utils import run_sql, compare_with_minimum, parse_sql_condition
from v6_summary_omop.sql_functions import cohort_count

def create_table_statement(table, variable):
    """ Create the table statement so that all tables become generic.
    """
    sql_statement = ""
    if table.lower() == OBSERVATION_TABLE:
        sql_statement = f"""SELECT observation_id AS id, concat(value_as_string, concept_name) AS 
            value FROM OBSERVATION AS o LEFT JOIN CONCEPT AS c ON c.concept_id = o.value_as_concept_id 
            WHERE o.observation_concept_id = {variable}
        """
    elif table.lower() == MEASUREMENT_TABLE:
        sql_statement = f"""SELECT measurement_id AS id, concat(value_as_number, concept_name) AS 
            value FROM MEASUREMENT AS m LEFT JOIN CONCEPT AS c ON c.concept_id = m.value_as_concept_id 
            WHERE measurement_concept_id = {variable}
        """
    elif table.lower() == CONDITION_TABLE:
        sql_statement = f"""SELECT condition_occurrence_id AS id, True AS value FROM CONDITION_OCCURRENCE 
            WHERE condition_concept_id = {variable} UNION ALL SELECT observation_id AS id, False AS value 
            FROM OBSERVATION WHERE observation_concept_id = {variable}
        """
    elif table.lower() == PERSON_TABLE:
        sql_statement = f"""SELECT person_id AS id, "{variable}" AS value FROM PERSON"""
    else:
        raise Exception(f"Non supported table requested: {table}")
    return sql_statement

def table_count(table_sql, db_client):
    """ Retireve the number of records in a table
    """
    sql_statement = f"""SELECT current_database(), COUNT("id") 
        FROM ({table_sql}) AS t WHERE "value" IS NOT NULL;"""
    result = run_sql(db_client, sql_statement)
    return result

def cohort_finder(cohort, db_client):
    """ Retrieve the results for the cohort finder option
    """
    id_column = ID_COLUMN in cohort and cohort[ID_COLUMN]
    sql_statement, sql_condition = cohort_count(
        id_column,
        cohort[COHORT_DEFINITION],
        cohort[TABLE],
    )
    # Check if the number of records in the table is enough
    count = table_count(cohort[TABLE], id_column, sql_condition, db_client)
    if int(count[1]) >= int(os.getenv(TABLE_MINIMUM) or TABLE_MINIMUM_DEFAULT):
        # If the total count for the cohort is below the accepted threshold
        # then the information won't be sent to the mater node
        result = run_sql(db_client, sql_statement)
        return ((result[0], compare_with_minimum(result[1])), sql_condition)
    else:
        return (
            {
                WARNING: f"Not enough records in database {count[0]}."
            }, 
            None
        )

def summary_results(columns, db_client):
    """ Retrieve the summary results for the requested functions
    """
    summary = {}
    sql_functions = None
    for column in columns:
        # validate the number of records available prior to obtaining any
        # summary statistics
        variable = column[VARIABLE]
        table_sql = create_table_statement(column[TABLE].upper(), column[VARIABLE])
        summary[variable] = {}
        # Count the number of non-null entries for the specific concept/variable
        # in the table
        result = table_count(table_sql, db_client)
        # Check if the number of entries is above the limit established
        if result and int(result[1]) >= int(os.getenv(TABLE_MINIMUM) or TABLE_MINIMUM_DEFAULT):
            if REQUIRED_FUNCTIONS in column:
                # construct the sql statement
                sql_functions = ""
                for function in column[REQUIRED_FUNCTIONS]:
                    if function.upper() not in sql_functions:
                        sql_functions += f"""{' ,' if sql_functions else ''}{function.upper()}(CAST("value" AS numeric))"""
                sql_statement = f"""SELECT {sql_functions} FROM ({table_sql}) AS t WHERE 
                    "value" IS NOT NULL;"""
                # execute the sql query and retrieve the results
                result = run_sql(db_client, sql_statement)
                # parse the results
                for i, function in enumerate(column[REQUIRED_FUNCTIONS]):
                    summary[variable][function] = result[i]

            if REQUIRED_METHODS in column:
                for method in column[REQUIRED_METHODS]:
                    sql_statement = method[CALL](table_sql, column)
                    summary[column[VARIABLE]][method[NAME]] = run_sql(
                        db_client, sql_statement, fetch_all = method[FETCH]==FETCH_ALL
                   )
        else:
           summary[column[VARIABLE]][WARNING] = f"Not enough records in database {result[0]}" + \
               " to execute the summary statistics." 
    return summary
