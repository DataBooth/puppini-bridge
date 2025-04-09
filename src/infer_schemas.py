import sqlglot
import duckdb
from pathlib import Path
from loguru import logger

# Infer schemas for each table based on their structure and generate a Puppini Bridge query dynamically.

DATA_DIR = Path("data")  # Directory containing the CSV files

# Define table names to infer schemas from DuckDB tables dynamically
tables = [
    "fact_sales",
    "fact_returns",
    "dim_product",
    "dim_customer",
    "dim_store",
    "dim_time",
]


def infer_schemas_and_generate_puppini_bridge_sql(tables, database_path):
    """
    Infer schemas from DuckDB tables and generate a Puppini Bridge SQL query.

    Args:
        tables (list): List of table names to infer schemas from.
        database_path (str): Path to the DuckDB database file. Default is in-memory (':memory:').

    Returns:
        str: Generated Puppini Bridge SQL query.
    """

    if not Path(database_path).exists():
        raise FileNotFoundError(f"Database file '{database_path}' does not exist!")
    else:
        logger.info(f"Database file '{database_path}' exists.")

    logger.info(f"Connecting to DuckDB database at '{database_path}'...")
    con = duckdb.connect(database=database_path, read_only=False)

    # Infer schemas by querying column names and types from DuckDB's information schema
    schemas = {}
    for table in tables:
        schema_query = f"PRAGMA table_info({table})"
        columns = con.execute(schema_query).fetchall()
        schemas[table] = {
            col[1]: col[2] for col in columns
        }  # col[1]: name; col[2]: type

    return con, schemas


# Generate Puppini Bridge SQL using SQLGlot based on inferred schemas


def generate_puppini_bridge_sql(schemas):
    union_queries = []
    for table_name, columns in schemas.items():
        logger.info(f"Generating SELECT query for table '{table_name}'...")
        primary_key = list(columns.keys())[
            0
        ]  # Assume first column is the primary key (e.g., sale_id)
        select_query = f"SELECT CAST({primary_key} AS VARCHAR) AS _KEY_{table_name}, '{table_name}' AS Stage FROM {table_name}"
        union_queries.append(select_query)
        logger.info(f"Generated SELECT query: {select_query}")

    # Combine all SELECT statements with UNION ALL
    union_all_query = " UNION ALL ".join(union_queries)
    logger.info(f"Generated UNION ALL query: {union_all_query}")

    # Wrap in CREATE OR REPLACE TABLE statement for Puppini Bridge creation
    create_table_query = f"CREATE OR REPLACE TABLE puppini_bridge AS {union_all_query}"
    logger.info(f"Generated Puppini Bridge SQL: {create_table_query}")

    return create_table_query


def main():
    database_path = str(DATA_DIR / "sales.duckdb")
    con, schemas = infer_schemas_and_generate_puppini_bridge_sql(tables, database_path)

    print(schemas)

    puppini_bridge_sql = generate_puppini_bridge_sql(schemas)

    # Execute the generated SQL in DuckDB to create the Puppini Bridge table
    con.execute(puppini_bridge_sql)


if __name__ == "__main__":
    main()
