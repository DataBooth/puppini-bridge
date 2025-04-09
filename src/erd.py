import duckdb
from pathlib import Path
from loguru import logger

# Define the database path
DATA_DIR = Path("data")
DATABASE_PATH = DATA_DIR / "sales.duckdb"


def generate_mermaid_erd(database_path):
    """
    Generate a Mermaid ERD from DuckDB tables.

    Args:
        database_path (Path): Path to the DuckDB database file.

    Returns:
        str: Mermaid ERD diagram as a string.
    """
    logger.info(f"Connecting to DuckDB database at '{database_path}'...")
    con = duckdb.connect(database=str(database_path), read_only=True)

    # Query for table and column metadata
    logger.info("Fetching table and column metadata...")
    table_columns_query = """
        SELECT 
            table_name, 
            column_name, 
            data_type 
        FROM information_schema.columns
        WHERE table_schema = 'main'
        ORDER BY table_name, ordinal_position;
    """
    table_columns = con.execute(table_columns_query).fetchall()

    # Query for foreign key relationships
    logger.info("Fetching foreign key relationships...")
    foreign_keys_query = """
        SELECT 
            fk_table.table_name AS from_table,
            fk_column.column_name AS from_column,
            pk_table.table_name AS to_table,
            pk_column.column_name AS to_column
        FROM information_schema.referential_constraints rc
        JOIN information_schema.key_column_usage fk_column
            ON rc.constraint_name = fk_column.constraint_name
        JOIN information_schema.table_constraints fk_table
            ON fk_column.table_name = fk_table.table_name AND fk_table.constraint_type = 'FOREIGN KEY'
        JOIN information_schema.key_column_usage pk_column
            ON rc.unique_constraint_name = pk_column.constraint_name
        JOIN information_schema.table_constraints pk_table
            ON pk_column.table_name = pk_table.table_name AND pk_table.constraint_type = 'PRIMARY KEY'
        WHERE fk_table.table_schema = 'main' AND pk_table.table_schema = 'main';
    """
    foreign_keys = con.execute(foreign_keys_query).fetchall()

    # Start building the Mermaid diagram
    mermaid_diagram = ["erDiagram"]

    # Add tables and columns to the diagram
    logger.info("Adding tables and columns to the diagram...")
    tables = {}
    for table, column, data_type in table_columns:
        if table not in tables:
            tables[table] = []
        tables[table].append(f"{data_type.lower()} {column}")

    for table, columns in tables.items():
        mermaid_diagram.append(f"  {table} {{")
        mermaid_diagram.extend([f"    {col}" for col in columns])
        mermaid_diagram.append("  }")

    # Add relationships to the diagram
    logger.info("Adding relationships to the diagram...")
    for from_table, from_column, to_table, to_column in foreign_keys:
        mermaid_diagram.append(
            f'  {from_table} }}o--o{{ {to_table} : "{from_column} -> {to_column}"'
        )

    # Combine all lines into a single Mermaid diagram string
    return "\n".join(mermaid_diagram)


def main():
    if not DATABASE_PATH.exists():
        logger.error(f"DuckDB database file '{DATABASE_PATH}' does not exist.")
        return

    logger.info("Generating Mermaid ERD...")
    erd_diagram = generate_mermaid_erd(DATABASE_PATH)

    # Save the Mermaid diagram to a file (optional)
    output_file = Path("er-diagram.mermaid")
    output_file.write_text(erd_diagram)
    logger.info(f"Mermaid ERD saved to '{output_file}'.")

    # Print the Mermaid diagram
    print("\nGenerated Mermaid ERD:")
    print(erd_diagram)


if __name__ == "__main__":
    main()
