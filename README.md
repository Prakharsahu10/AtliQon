<div align="center">

# AtliQon · FMCG Data Pipeline

**A Databricks Lakehouse pipeline for bringing Sports Bar sales data into a shared FMCG model.**

[Architecture](#architecture) · [Pipeline flow](#pipeline-flow) · [Runbook](#runbook) · [Data model](#data-model) · [Operational notes](#operational-notes)

</div>

---

## Overview

This repository contains Databricks notebooks that ingest customer, product, pricing, and order CSV files from S3, refine them into Delta tables, and merge the results into parent-company dimensions and facts.

The notebooks implement a **Bronze → Silver → Gold** layout:

- **Bronze** keeps the source rows with ingestion metadata.
- **Silver** cleans and enriches records, including dimension lookups for pricing and orders.
- **Gold** selects the child-model outputs and merges them into shared parent tables.

This is an orchestration-by-notebook project: there is no application package, dependency manifest, or job definition in the repository. It expects a Databricks workspace with Spark, Delta Lake, Unity Catalog, `dbutils`, the source S3 paths, and the parent-company tables already available.

## Architecture

```mermaid
flowchart LR
    subgraph Source["S3 · sportsbar-dp3"]
        C[customers CSV]
        P[products CSV]
        G[gross_price CSV]
        OL[orders / landing CSVs]
    end

    subgraph Setup["Databricks · setup"]
        CAT[Create fmcg catalog<br/>bronze · silver · gold schemas]
        DATE[Build monthly dim_date]
        U[Schema-name utility]
    end

    subgraph Dimensions["Dimension pipelines"]
        CB[bronze.customers]
        CS[silver.customers]
        CG[gold.sb_dim_customers]
        PB[bronze.products]
        PS[silver.products]
        PG[gold.sb_dim_products]
        GB[bronze.gross_price]
        GS[silver.gross_price]
        GG[gold.sb_dim_gross_price]
    end

    subgraph Facts["Order fact pipeline"]
        OB[bronze.orders]
        STB[bronze.staging_orders<br/>(incremental only)]
        OS[silver.orders]
        STS[silver.staging_orders<br/>(incremental only)]
        OG[gold.sb_fact_orders]
    end

    subgraph Parent["Shared parent model · fmcg.gold"]
        DC[dim_customers]
        DP[dim_products]
        DGP[dim_gross_price]
        FO[fact_orders<br/>monthly grain]
    end

    CAT --> CB
    CAT --> PB
    CAT --> GB
    CAT --> OB
    U -. used by notebooks .-> Dimensions
    U -. used by notebooks .-> Facts
    DATE --> Parent

    C --> CB --> CS --> CG --> DC
    P --> PB --> PS --> PG --> DP
    G --> GB --> GS --> GG --> DGP
    OL --> OB --> OS --> OG --> FO
    PS -->|product_id → product_code| GS
    PS -->|product_id → product_code| OS
    OL -. incremental landing batch .-> STB --> OS
    STB --> STS --> OG
```

The `sb_` tables are child or staging-shaped Gold outputs. Dimension jobs merge them into the shared dimension tables. The order job aggregates the child daily fact to month, product, and customer before merging it into `fmcg.gold.fact_orders`.

## Repository layout

```text
consolidated_pipeline/
├── 1_setup/
│   ├── setup_catalog.ipynb          # Creates catalog and Bronze/Silver/Gold schemas
│   ├── utilities.ipynb              # Shared schema-name variables
│   └── dim_date_table_creation.ipynb # Builds the monthly date dimension
├── 2_dimension_data_processing/
│   ├── 1_customer_data_processing.ipynb
│   ├── 2_products_data_processing.ipynb
│   └── 3_pricing_data_processing.ipynb
└── 3_fact_data_processing/
    ├── 1_full_load_fact.ipynb
    └── 2_incremental_load_fact.ipynb
```

## Pipeline flow

### 1. Set up the catalog

Run `setup_catalog.ipynb` to create the `fmcg` catalog and its `bronze`, `silver`, and `gold` schemas. Run `dim_date_table_creation.ipynb` to create `fmcg.gold.dim_date`. The date notebook currently generates month-start rows from **2024-01-01 through 2025-12-01**.

`utilities.ipynb` defines the three schema-name variables used via Databricks `%run` imports. Notebooks still use some literal `fmcg` references, so changing the widget catalog alone does not make every downstream reference portable.

### 2. Process dimensions

Each dimension notebook reads a CSV source from `s3://sportsbar-dp3/<data_source>/*.csv`, writes a Bronze Delta table, applies Silver transformations, and writes a child Gold table. It then merges the child output into a parent dimension.

| Notebook | Default source | Main Silver work | Child Gold → parent target |
|---|---|---|---|
| `1_customer_data_processing` | `customers` | Deduplicates by customer ID, trims and title-cases names, normalizes city values, and adds customer, market, platform, and channel attributes | `gold.sb_dim_customers` → `gold.dim_customers` |
| `2_products_data_processing` | `products` | Deduplicates by product ID, standardizes category/product spelling, derives division and variant, and creates a SHA-256 product code | `gold.sb_dim_products` → `gold.dim_products` |
| `3_pricing_data_processing` | `gross_price` | Parses multiple month formats, normalizes invalid/negative prices, and joins product codes from Silver products | `gold.sb_dim_gross_price` → `gold.dim_gross_price` |

Run products before pricing because pricing needs `fmcg.silver.products`. Products should also be ready before order facts, which join on `product_id` to obtain `product_code`.

### 3. Load order facts

Both fact notebooks read order CSVs under `s3://sportsbar-dp3/orders/landing/`, preserve ingestion metadata in Bronze, clean the order date and identifiers, remove duplicate rows, and enrich orders with product codes from `fmcg.silver.products`. The child Silver and Gold tables retain the order-level detail.

- **Full load** (`1_full_load_fact.ipynb`) appends landing data to Bronze, upserts into the child Silver and Gold tables, then aggregates the child Gold data to monthly grain and merges it into `fmcg.gold.fact_orders`.
- **Incremental load** (`2_incremental_load_fact.ipynb`) exits early when there are no CSVs in landing. Otherwise, it appends the batch to Bronze, writes Bronze and Silver staging tables, updates child Silver and Gold, identifies affected months, recomputes those months from the child Gold fact, and merges the recalculated rows into the parent fact. It drops the staging tables at the end.

The incremental notebook moves files from `landing/` to `processed/` as part of ingestion. Use that notebook for recurring batches; use the full-load notebook when the intended input is the full landing set.

## Data model

### Child pipeline tables

| Layer | Customer | Product | Pricing | Orders |
|---|---|---|---|---|
| Bronze | `fmcg.bronze.customers` | `fmcg.bronze.products` | `fmcg.bronze.gross_price` | `fmcg.bronze.orders` |
| Silver | `fmcg.silver.customers` | `fmcg.silver.products` | `fmcg.silver.gross_price` | `fmcg.silver.orders` |
| Gold | `fmcg.gold.sb_dim_customers` | `fmcg.gold.sb_dim_products` | `fmcg.gold.sb_dim_gross_price` | `fmcg.gold.sb_fact_orders` |

The fact incremental job also uses temporary `fmcg.bronze.staging_orders` and `fmcg.silver.staging_orders` tables.

### Parent tables expected by the notebooks

The shared model is assumed to contain `fmcg.gold.dim_customers`, `dim_products`, `dim_gross_price`, and `fact_orders`. These are not created in this repository; the notebooks merge into them and expect compatible columns and keys.

At the parent fact boundary, order rows are grouped by:

```text
month_start date × product_code × customer_code
```

`sold_quantity` is summed for that grain. The parent dimension key conventions are `customer_code` for customers and `product_code` for products and pricing.

## Runbook

1. Confirm Databricks has access to `s3://sportsbar-dp3/` and the required parent Gold tables exist.
2. Import or sync `consolidated_pipeline/` into the workspace path expected by `%run` (`/Workspace/consolidated_pipeline/...`).
3. Run `1_setup/setup_catalog.ipynb` and `1_setup/dim_date_table_creation.ipynb` if the catalog, schemas, or date dimension need initialization.
4. Run customer and product dimension notebooks. Run pricing after products.
5. Run the full fact notebook for the intended baseline load, or the incremental fact notebook for newly landed files.
6. Review row counts, unmatched product IDs, date parsing results, and parent table merges in the notebook output.

Dimension widgets expose `catalog` and `data_source`; fact notebooks expose the same values and use the corresponding `landing/` and `processed/` order folders. Several notebook joins and parent table references are currently hard-coded to `fmcg`, `products`, or `orders`.

## Operational notes

These notes reflect the implementation in the notebooks and are useful to check before production scheduling:

- **Overwrites:** Dimension Bronze, Silver, and child Gold tables are written in `overwrite` mode. Re-running them replaces those layer tables before merging into the parent. Preserve source snapshots if historical Bronze retention is needed.
- **File movement and recovery:** Fact notebooks move files after Bronze ingestion, before downstream Silver/Gold work finishes. A later failure may leave files in `processed/`; retry procedures should account for that state.
- **Incremental month refresh:** The incremental fact path recalculates affected months from the child Gold table, which supports late-arriving changes within those months. Confirm that child Gold contains the complete rows for each affected month.
- **Pricing merge grain:** Pricing ranks rows by product and year, but the parent merge condition matches on `product_code` alone. If the source yields more than one year for a product, verify that the intended year-selection and parent key match the actual parent schema.
- **Fallback handling:** Customer and order transforms assign defaults for unresolved values (for example, assumed customer cities and customer ID `999999`). Track those records if downstream analysis needs to distinguish source values from fallbacks.
- **Change Data Feed:** Several Delta writes enable Change Data Feed, but these notebooks do not consume the feed; they currently use table reads and Delta `MERGE` operations.
- **Date coverage:** Extend the date dimension's configured end date as order data grows beyond December 2025.

## Technology

- Databricks notebooks and `dbutils`
- Apache Spark / PySpark
- Delta Lake tables and `MERGE`
- Unity Catalog
- Amazon S3 CSV landing paths

---

<div align="center">
<sub>Notebook-first data engineering project · Bronze → Silver → Gold</sub>
</div>
