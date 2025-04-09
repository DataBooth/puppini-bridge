from loguru import logger
import duckdb
from pathlib import Path
import pandas as pd

# Define the data directory
DATA_DIR = Path("data")  # Directory containing the CSV files


def load_csv_to_duckdb(csv_files, force=False, database_path=":memory:"):
    """
    Load multiple CSV files into DuckDB as tables with auto schema inference.

    Args:
        csv_files (dict): A dictionary where keys are table names and values are Path objects pointing to CSV files.
                          Example: {"fact_sales": Path("data/fact_sales.csv"), "dim_product": Path("data/dim_product.csv")}
        database_path (str): Path to the DuckDB database file. Default is in-memory (':memory:').

    Returns:
        duckdb.Connection: A connection to the DuckDB database with the loaded tables.
    """
    logger.info(f"Connecting to DuckDB database at '{database_path}'...")

    if Path(database_path).exists() and not force:
        logger.warning(
            f"DuckDB database '{database_path}' already exists. Connecting to existing. Use 'force=True' to overwrite."
        )
        return duckdb.connect(database=database_path, read_only=False)

    con = duckdb.connect(database=database_path, read_only=False)
    for table_name, csv_path in csv_files.items():
        if not csv_path.exists():
            logger.error(
                f"CSV file '{csv_path}' does not exist. Skipping table '{table_name}'."
            )
            continue

        logger.info(
            f"Loading CSV file '{csv_path}' into DuckDB table '{table_name}'..."
        )
        query = (
            f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')"
        )
        con.execute(query)
        logger.info(f"Table '{table_name}' created successfully.")

    return con


def main():
    # Define the CSV files and their corresponding table names
    csv_files = {
        "fact_sales": DATA_DIR / "fact_sales.csv",
        "fact_returns": DATA_DIR / "fact_returns.csv",
        "dim_product": DATA_DIR / "dim_product.csv",
        "dim_customer": DATA_DIR / "dim_customer.csv",
        "dim_store": DATA_DIR / "dim_store.csv",
        "dim_time": DATA_DIR / "dim_time.csv",
    }

    # Configure Loguru logging
    LOG_FILE = Path("logs") / "app.log"
    LOG_FILE.parent.mkdir(exist_ok=True)  # Ensure logs directory exists
    logger.add(LOG_FILE, rotation="1 MB", retention="10 days", level="INFO")

    logger.info("Starting CSV loading process...")

    # Load CSV files into DuckDB
    con = load_csv_to_duckdb(csv_files, database_path=str(DATA_DIR / "sales.duckdb"))

    # Verify that tables are loaded correctly (optional)
    logger.info("Verifying sample data from 'fact_sales' table...")
    sample_data = con.sql("SELECT * FROM fact_sales").df()
    logger.info(f"Sample data from 'fact_sales': {sample_data}")


if __name__ == "__main__":
    main()
