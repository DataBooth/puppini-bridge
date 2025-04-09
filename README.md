# `puppini-bridge`

Puppini bridge example (with SQLGlot + DuckDB)

# Puppini Bridge: Universal Star Schema Example

## Overview

This project demonstrates the creation of a **Puppini Bridge** (Universal Star Schema) using DuckDB and Python. The Puppini Bridge is a unified schema that simplifies multi-fact analysis by combining all fact and dimension tables into a single table with consistent relationships. This repository includes code to:

* Load data from CSV files into DuckDB.
* Infer schemas dynamically.
* Generate SQL for creating a Puppini Bridge.
* Visualize the schema as an Entity-Relationship Diagram (ERD) using Mermaid.

***

## Motivation: Why Use a Puppini Bridge?

### The Problem

Traditional star schemas work well when analyzing data from a single fact table. However, in many real-world scenarios, multiple fact tables (e.g., `fact_sales` and `fact_returns`) need to be analyzed together. This introduces challenges such as:

1. **Complex Joins**:
   * Queries often require manual joins between fact tables and shared dimension tables.
   * Users must understand the relationships between tables, leading to errors or inefficiencies.

2. **Fan Traps**:
   * When analyzing multiple fact tables simultaneously, incorrect relationships can result in duplicate or missing data.

3. **Self-Service BI Complexity**:
   * Business users may struggle to write queries that correctly combine data from multiple fact tables.

### The Solution: Puppini Bridge

The Puppini Bridge solves these problems by:

* Acting as a **centralized table** that combines all primary keys from fact and dimension tables.
* Standardizing relationships between entities, preventing fan traps and simplifying queries.
* Supporting multi-fact analysis without requiring users to understand complex joins.

***

## How This Example Illustrates the Problem and Solution

### Scenario

This example uses a retail dataset with two fact tables (`fact_sales` and `fact_returns`) and four dimension tables (`dim_product`, `dim_customer`, `dim_store`, and `dim_time`). The goal is to analyze both sales and returns data together, grouped by product, customer, store, or time.

### Challenges Without Puppini Bridge

Without the Puppini Bridge:

* Queries would involve manually joining multiple fact tables with shared dimensions.
* Users would need to explicitly define relationships between tables for each query.
* Errors like fan traps could occur if relationships are not properly managed.

### Benefits of Puppini Bridge

With the Puppini Bridge:

1. **Simplified Queries**:
   * Users can start their analysis from the bridge table without worrying about joins.
   * Example query: Summing sales and returns grouped by product becomes straightforward.

2. **Preventing Fan Traps**:
   * The bridge ensures that each row corresponds to the correct relationships between facts and dimensions.

3. **Unified Schema**:
   * All entities (facts and dimensions) are represented in one table, enabling seamless multi-fact analysis.

***

## Do We Need the `dim_time` Table?

### Purpose of `dim_time`

The `dim_time` table provides additional context for dates, such as month or year. It is useful when performing time-based aggregations or filtering (e.g., "sales by month" or "returns in 2025").

### Is It Necessary?

In this example:

* If your analysis only requires raw dates (e.g., `fact_sales.date`), you can skip the `dim_time` table and directly use the date columns from fact tables.
* If you need higher-level time attributes (e.g., month or year), then `dim_time` is valuable for grouping or filtering data.

### Recommendation

Keep the `dim_time` table if your analysis involves aggregations like "sales by month" or "returns by year." Otherwise, it can be omitted for simplicity.

***

## Project Structure

```

├── data/
│   ├── fact_sales.csv
│   ├── fact_returns.csv
│   ├── dim_product.csv
│   ├── dim_customer.csv
│   ├── dim_store.csv
│   ├── dim_time.csv
├── src/
│   ├── infer_schemas.py       \# Code for inferring schemas and generating Puppini Bridge SQL
│   ├── erd_generator.py       \# Code for generating Mermaid ERD diagrams
├── README.md                  \# Project documentation

```

***

## Setup Instructions

### Prerequisites

1. Python 3.8+
2. DuckDB installed (`pip install duckdb`)
3. Loguru installed (`pip install loguru`)
4. CSV files placed in the `data/` directory.

### Steps

1. Clone this repository:

```

git clone https://github.com/yourusername/puppini-bridge.git
cd puppini-bridge

```

2. Install dependencies:

```

pip install duckdb loguru sqlglot pathlib

```

3. Run the schema inference script:

```

python src/infer_schemas.py

```

4. Run the ERD generator script to create a Mermaid diagram:

```

python src/erd_generator.py

```

***

## Example Output

### Generated Puppini Bridge SQL:

```

CREATE OR REPLACE TABLE puppini_bridge AS
SELECT CAST(sale_id AS VARCHAR) AS _KEY_fact_sales, 'fact_sales' AS Stage FROM fact_sales
UNION ALL
SELECT CAST(return_id AS VARCHAR) AS _KEY_fact_returns, 'fact_returns' AS Stage FROM fact_returns
UNION ALL
SELECT CAST(product_id AS VARCHAR) AS _KEY_dim_product, 'dim_product' AS Stage FROM dim_product
UNION ALL
SELECT CAST(customer_id AS VARCHAR) AS _KEY_dim_customer, 'dim_customer' AS Stage FROM dim_customer
UNION ALL
SELECT CAST(store_id AS VARCHAR) AS _KEY_dim_store, 'dim_store' AS Stage FROM dim_store
UNION ALL
SELECT CAST(date AS VARCHAR) AS _KEY_dim_time, 'dim_time' AS Stage FROM dim_time;

```

### Generated Mermaid ERD Diagram:

```

erDiagram
fact_sales {
bigint sale_id
bigint product_id
bigint customer_id
bigint store_id
date date
bigint units_sold
double sales_amount
}
fact_returns {
bigint return_id
bigint product_id
bigint customer_id
bigint store_id
date date
bigint units_returned
}
dim_product {
bigint product_id
varchar name
varchar category
}
dim_customer {
bigint customer_id
varchar name
varchar region
}
dim_store {
bigint store_id
varchar name
varchar city
}
dim_time {
date date
bigint month
bigint year
}
fact_sales }}o--o{{ dim_product : "product_id -> product_id"
fact_sales }}o--o{{ dim_customer : "customer_id -> customer_id"
fact_sales }}o--o{{ dim_store : "store_id -> store_id"
fact_sales }}o--o{{ dim_time : "date -> date"

```

***

## Next Steps

1. Extend this example to include additional datasets or facts.
2. Automate rendering of Mermaid diagrams into images using tools like [Mermaid CLI](https://github.com/mermaid-js/mermaid-cli).
3. Explore how the Puppini Bridge can simplify queries in self-service BI tools like Tableau or Power BI.

***

## License

This project is licensed under [MIT License](LICENSE).
