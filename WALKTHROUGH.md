# Data Engineering Code Challenge — Solution Documentation

## Overview

This project implements a **retail sales analytics pipeline** using **Python and PySpark**. The goal is to ingest raw sales, product, and store data, perform data quality checks, transform and enrich the data, and generate actionable KPIs in an automated, maintainable, and testable way.
---

> **Note:**  
> The `data/` and `output/` directories are included in this repository *solely for evaluation purposes*.  
> In a production environment, raw and processed data would typically reside in external storage systems such as cloud storage containers (e.g., AWS S3, Azure Blob Storage, or Google Cloud Storage) and **not** be version-controlled in Git.

## Project Design

### Architecture Layers

The solution follows a layered architecture inspired by modern data engineering best practices:

| Layer              | Description                                          | Location in Project             |
|--------------------|------------------------------------------------------|--------------------------------|
| **Raw Layer**      | Source CSV files ingested as-is                      | `data/raw/`                    |
| **Data Quality (DQ) Layer** | Null checks, duplicate detection, and schema enforcement | `data_quality/`                |
| **Clean Layer**    | Cleaned data after applying DQ rules                 | `output/clean/`                |
| **Enterprise Layer** | Enriched data joining sales, products, and stores  | `output/enriched/`             |
| **Metrics Layer**  | Calculated KPIs and insights for analytics/reporting | `output/metrics/`              |

---

### Code Organization

| Folder              | Purpose                                                                                  |
|---------------------|------------------------------------------------------------------------------------------|
| `config/`           | YAML configuration file with input/output paths, schema, and source system info         |
| `data/raw/`         | Raw CSV source files: products, sales, stores                                           |
| `data_quality/`     | Output folders for duplicates and nulls detection                                       |
| `output/`           | Final output folders for clean, enriched data and metrics                               |
| `source_code/`              | Source code for loading, transforming, and utilities                                    |
| `source_code/data_prep/`    | Data loading classes and base loader                                                    |
| `source_code/data_transform/` | Transformation and analysis jobs, including KPI calculations                           |
| `utils/`            | Helper modules: config loader, date utils, IO utils, DQ, logging setup                       |
| `tests/`            | PySpark unit tests using `chispa` package                                              |

---

### Core Components

- **Config Loader**: Parses the YAML config file to provide paths, schema, and parameters.
- **Data Loader**: Implements data loading for all source files simultaneously implementing the DQ checks.
- **Enrich Builder**: Joins clean datasets to create an enriched enterprise layer.
- **KPI Calculator**: Calculates KPIs such as monthly sales insights and total revenue by store.
- **Logging**: Uses Python `logging` module for info/debug messages (no `print()` in production).
- **Tests**: Written using PySpark and `chispa` to verify DataFrame transformations and DQ logic.
- **CI/CD**: GitHub Actions workflow automates tests and linting on every push and PR.

---

## How to Run

### Prerequisites

- Python 3.8 or above
- Java 8+ (required for PySpark)
- Apache Spark installed or use `pyspark` package
- Install Python dependencies:

```bash
pip install -r requirements.txt
```

### Configuration

- Edit `config/master_config.yml` to specify:
  - Input source paths (CSV files)
  - Output folders for clean, enriched, and metrics data
  - Source system info and schema definitions

### Run the Application

Use the entry point script to run the full pipeline:

```bash
python -m main config/master_config.yml
```

This will:

1. Load raw CSVs from the paths specified.
2. Run data quality checks (null and duplicate detection).
3. Save cleaned data in `output/clean/`.
4. Enrich data into the enterprise layer in `output/enriched/`.
5. Calculate KPIs and save them under `output/metrics/<rundate>/`.

---

### Output Folder Structure

```
output/
  clean/
    products/
    sales/
    stores/
  enriched/
    enterprise_layer.parquet
  metrics/
    160625/  # Date folder created dynamically
      monthly_sales_insight.csv
      total_revenue_by_store.csv
```

---

### Running Tests

Run PySpark unit tests with `chispa`:

```bash
pytest tests/
```

Tests verify:

- Data quality checks (nulls, duplicates)
- Data transformation correctness
- KPI calculations

---

### Logging

- All major steps log info messages via Python’s `logging` module.
- Logs include data loading start/end, quality check summaries, transformation progress, and KPI calculations.
- Logging configuration is in `utils/logger.py`.

---

### Continuous Integration (CI)

- GitHub Actions workflow automatically runs:
  - Code linting with `flake8`
  - Unit tests with `pytest`
- Triggered on every push or pull request.
- Ensures code quality and reduces regressions.

---

### Assumptions

- **Input Data Quality**:
  - All datasets (`sales`, `products`, and `stores`) are generally clean and schema-compliant.
  - `price` is of type `DoubleType` and assumed to be **non-null and non-negative**.
  - `quantity` is assumed to be an integer and **non-null**.
  - `product_id` and `store_id` are present and valid foreign keys for joining.

- **Date Parsing**:
  - The `transaction_date` field may contain **multiple formats**, such as:
    - `dd/MM/yyyy`, `MM/dd/yyyy`, `yyyy-MM-dd`, `MMMM dd, yyyy`, etc.
  - Dates are parsed via a **custom parser** that matches formats using regex + `to_date`.
  - If a date cannot be parsed, it is set to `1900-01-01`.
  - All rows with `null` transaction dates are either excluded from aggregations or logged for inspection.

- **Enriched Dataset**:
  - Combines `sales`, `products`, and `stores` into a single unified view.
  - Always includes the following columns:
    - `transaction_id`, `store_name`, `location`, `product_name`, `category`, `quantity`, `transaction_date`, `price`, `price_category`.
  - `price_category` is derived using a PySpark UDF based on price thresholds:
    - `Low`: `< 20`, `Medium`: `20–100`, `High`: `> 100`.
  - Product prices vary across transactions (due to discounts, promotions, or dynamic pricing).
  - Therefore, price categorization (`Low`, `Medium`, `High`) is applied at the **transaction level** based on the price recorded in each transaction row, not as a fixed attribute of the product.

- **Revenue Calculations**:
  - Revenue = `quantity * price`.
  - Aggregated revenue is **rounded to 2 decimal places** using `round()`.

- **Data Export**:
  - Enriched dataset is saved in **Parquet format**, partitioned by `category` and `transaction_date`.
  - Aggregated revenue by store/category is saved as **CSV**.
  - All output paths are assumed to be **writable**, and existing files may be **overwritten**.

- **Config and Code Structure**:
  - Supported date formats can be configured in a `.yaml` file for flexibility.
  - Parsing, saving, and transformation logic is modularized across `source_code/` and `utils/`.


---

## Future Improvements

- Add parameterization for date ranges in KPIs.
- Enable dynamic schema validation from external sources.
- Add data lineage and monitoring dashboards.
- Containerize app for consistent deployment.

---

### Known Issues / Limitations

- Supports only `.csv` input files.
- Assume all input files fit in memory (no chunked processing yet).
- Schema definitions must be manually maintained in the config YAML.
- No automated backfilling of historical KPI metrics.

---

### Frequently Asked Questions

**Q: Can I run the pipeline on just one dataset?**  
A: Yes. You can modify the YAML config to point only to the dataset you wish to run.

**Q: How is the run date for metrics determined?**  
A: The current date (from `datetime.now().strftime('%d%m%y')`) is appended to the metric filenames and folder path.

**Q: How do I add a new KPI?**  
A: Create a function in `analysis_job.py`, update `main.py` to call it, and optionally write a test in `tests/`.
