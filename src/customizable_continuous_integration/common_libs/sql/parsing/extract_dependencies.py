"""Extract dependencies from a SQL statement

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  03/03/2025   Ryan, Gao       Initial creation
"""

import sqlglot


def extract_sql_select_statement_dependencies(sql: str, exclusions: set[str], dialect: str = "bigquery") -> set[str]:
    """
    Extract dependencies from a SQL SELECT statement

    :param sql: The SQL statement
    :param exclusions: The set of exclusions
    :return: The set of dependencies
    """
    parsed_sql_ast = sqlglot.parse_one(sql, dialect=dialect)
    if type(parsed_sql_ast) is not sqlglot.exp.Select:
        raise ValueError("The SQL statement must be a SELECT statement")
    ret = set()
    for node in parsed_sql_ast.find_all(sqlglot.exp.CTE):
        exclusions.add(node.alias_or_name)
    for node in parsed_sql_ast.find_all(sqlglot.exp.Table):
        full_name = f"{node.catalog+'.' if node.catalog else ''}{node.db+'.' if node.db else ''}{node.alias_or_name}"
        if full_name in exclusions:
            continue
        ret.add(full_name)
    return ret
