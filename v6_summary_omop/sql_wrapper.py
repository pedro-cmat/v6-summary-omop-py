import os

from v6_summary_omop.constants import *
from v6_summary_omop.utils import compare_with_minimum, run_sql, parse_sql_condition

def get_database_name(db_client):
    """ Retrieve the database name.
    """
    return run_sql(db_client, "SELECT current_database();")[0]

def create_table_statement(table, variable, return_value=True, condition=None):
    """ Create the table statement so that all tables become generic.
    """
    sql_statement = ""
    if table.lower() == OBSERVATION_TABLE:
        value_statement = f", concat(value_as_string, concept_name) AS {VALUE}"
        sql_statement = f"""SELECT person_id AS id {value_statement if return_value else ""} 
            FROM OBSERVATION AS o LEFT JOIN CONCEPT AS c ON c.concept_id = o.value_as_concept_id 
            WHERE o.observation_concept_id = {variable} {f"AND {condition}" if condition else ""}
        """
    elif table.lower() == MEASUREMENT_TABLE:
        value_statement = f", concat(value_as_number, concept_name) AS {VALUE}"
        sql_statement = f"""SELECT person_id AS id, {value_statement if return_value else ""} 
            FROM MEASUREMENT AS m LEFT JOIN CONCEPT AS c ON c.concept_id = m.value_as_concept_id 
            WHERE measurement_concept_id = {variable} {f"AND {condition}" if condition else ""}
        """
    elif table.lower() == CONDITION_TABLE:
        sql_statement = f"""SELECT person_id AS id {", True AS {VALUE}" if return_value else ""} 
            FROM CONDITION_OCCURRENCE WHERE condition_concept_id = {variable} 
            UNION ALL SELECT person_id AS id {", False AS {VALUE}" if return_value else ""} 
            FROM OBSERVATION WHERE observation_concept_id = {variable}
        """
    elif table.lower() == PERSON_TABLE:
        value_statement = f', "{variable}" AS {VALUE}'
        sql_statement = f"""SELECT person_id AS id {value_statement if return_value else ""} 
            FROM PERSON {f"WHERE {condition}" if condition else ""}"""
    else:
        raise Exception(f"Non supported table requested: {table}")
    return sql_statement

def table_count(table_sql, db_client):
    """ Retireve the number of records in a table
    """
    sql_statement = f"""SELECT current_database(), COUNT("id") 
        FROM ({table_sql}) AS t WHERE "{VALUE}" IS NOT NULL;"""
    result = run_sql(db_client, sql_statement)
    return result

def cohort_selection(definition, db_client):
    """ Select the persons id that fall into the citeria provided
        for thr cohort.
    """
    sql_condition = []
    for component in definition:
        value = VALUE in component
        if component[TABLE].lower() == OBSERVATION_TABLE:
            condition = ""
            if value:
                if isinstance(component[VALUE], str):
                    condition = f"value_as_string {component[OPERATOR]} {component[VALUE]}"
                else:
                    condition = f"value_as_number {component[OPERATOR]} {component[VALUE]}"
            else:
                condition = f"concept_id {component[OPERATOR]} {component[CONCEPT_ID]}"
            sql_condition.append(create_table_statement(
                OBSERVATION_TABLE,
                component[VARIABLE],
                return_value=False,
                condition=condition
            ))
        elif component[TABLE].lower() == MEASUREMENT_TABLE:
            sql_condition.append(create_table_statement(
                MEASUREMENT_TABLE,
                component[VARIABLE],
                return_value=False,
                condition=f"value_as_number {component[OPERATOR]} {component[VALUE]}" \
                    if value else f"concept_id {component[OPERATOR]} {component[CONCEPT_ID]}"
            ))
        elif component[TABLE] == CONDITION_TABLE:
            # TODO: complete for Condition
            pass
        elif component[TABLE].lower() == PERSON_TABLE:
            sql_condition.append(create_table_statement(
                PERSON_TABLE,
                component[VARIABLE],
                return_value=False,
                condition=f"""{component[VARIABLE]} {component[OPERATOR]} {component[VALUE]}"""
            ))
        else:
            raise Exception(f"Non supported table requested for cohort: {component[TABLE]}")
    result = run_sql(
       db_client,
       f"""SELECT t.id FROM ({" INTERSECT ".join(sql_condition)}) AS t;""",
       fetch_all=True
    )
    return tuple([person_id[0] for person_id in result])

def cohort_finder(cohort, db_client):
    """ Retrieve the results for the cohort finder option
    """
    # Check if the number of records in the table is enough
    person_ids = cohort_selection(cohort[COHORT_DEFINITION], db_client)
    # If the total count for the cohort is below the accepted threshold
    # then the information won't be sent to the master node
    if len(person_ids) >= int(os.getenv(TABLE_MINIMUM) or TABLE_MINIMUM_DEFAULT):
        return (
            (get_database_name(db_client), compare_with_minimum(len(person_ids))),
            person_ids
        )
    else:
        return ({
                WARNING: f"Not enough records in the database."
        }, None)

def summary_results(columns, cohort_ids, db_client):
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
                        sql_functions += f"""{' ,' if sql_functions else ''}{function.upper()}(CAST("{VALUE}" AS numeric))"""
                sql_statement = f"""SELECT {sql_functions} FROM ({table_sql}) AS t WHERE 
                    "{VALUE}" IS NOT NULL {parse_sql_condition(cohort_ids)};"""
                # execute the sql query and retrieve the results
                result = run_sql(db_client, sql_statement)
                # parse the results
                for i, function in enumerate(column[REQUIRED_FUNCTIONS]):
                    summary[variable][function] = result[i]

            if REQUIRED_METHODS in column:
                for method in column[REQUIRED_METHODS]:
                    sql_statement = method[CALL](table_sql, cohort_ids, column)
                    summary[column[VARIABLE]][method[NAME]] = run_sql(
                        db_client, sql_statement, fetch_all = method[FETCH]==FETCH_ALL
                   )
        else:
           summary[column[VARIABLE]][WARNING] = f"Not enough records in database {result[0]}" + \
               " to execute the summary statistics." 
    return summary
