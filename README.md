# Data Engineering Code Challenge
Data Engineering Code Challenge (PySpark)

## Objective:
The purpose of this challenge is to evaluate your skills in data engineering, particularly in working with PySpark for data processing and transformation tasks. This challenge mimics real-world scenarios you might encounter in the role.

### Technical information:
- Use **Python** and **PySpark** [https://spark.apache.org/docs/latest/api/python/getting_started/install.html]
- Use the following package for PySpark tests - https://github.com/MrPowers/chispa - the application **needs** to have tests.
- **Do not use** notebooks for final delivery, like **Jupyter** for instance. While these are good for interactive work and/or prototyping in this case they shouldn't be used.
- The application should have an automated build pipeline using GitHub Actions.
- Follow best practices by using descriptive commit messages.
- Use **logging** to show information about what the application is doing, avoid using **print** statements, but feel free to use them in the tests for your own debugging purposes. However, they should not be there in the final version.
- Use type hints and docstrings as much as possible to enhance code documentation and readability.
- Consider using linters and code formatters and have it the Continuous Integration step of your automated build.

## Background:
You work for a company that analyzes retail sales data from various stores. Your task is to process raw transactional data, generate insights, and save the transformed data for further analysis.

The raw data consists of:
- A **Sales Transactions** dataset with information about products sold, quantities, and prices.
- A **Products** dataset containing product details.
- A **Stores** dataset with details about stores.

### Data Description
1. **Sales Transactions** Dataset (sales.csv):
    - ```transaction_id```: Unique identifier for the transaction.
    - ```store_id```: ID of the store where the transaction occurred.
    - ```product_id```: ID of the product sold.
    - ```quantity```: Number of units sold.
    - ```transaction_date```: Date of the transaction (format: YYYY-MM-DD).
    - ```price```: Price per unit.
2. **Products** Dataset (products.csv):
    - ```product_id```: Unique identifier for the product.
    - ```product_name```: Name of the product.
    - ```category```: Category of the product.
3. **Stores** Dataset (stores.csv):
    - ```store_id```: Unique identifier for the store.
    - ```store_name```: Name of the store.
    - ```location```: Location of the store.

## Tasks
You need to perform the following tasks using PySpark:
1. Part 1: **Data Preparation**
- Load the three datasets (sales.csv, products.csv, and stores.csv) into PySpark DataFrames.
- Perform basic data validation:
    - Check for missing or null values as well as inconsistencies in the data format.
    - Identify and handle duplicates, if any.
    - Enforce the appropriate data types for all columns.

2. Part 2: **Data Transformation**

    **Sales Aggregation**:
    - Calculate the total revenue for each store (store_id) and each product category.
    - Output: DataFrame with store_id, category, and total_revenue.

    **Monthly Sales Insights**:
    - Calculate the total quantity sold for each product category, grouped by month.
    - Output: DataFrame with year, month, category, and total_quantity_sold.

    **Enrich Data**:
    - Combine the sales, products, and stores datasets into a single enriched dataset with the following columns:
```transaction_id```, ```store_name```, ```location```, ```product_name```, ```category```, ``quantity``, ``transaction_date``, and ```price```.

    **PySpark UDF**:
    - Implement a PySpark UDF to categorize products based on the following price ranges:

        ```
        Low: price < 20
        Medium: 20 <= price <= 100
        High: price > 100
        ```

    - Add a column price_category to the enriched dataset and save it as an additional output.



3. Part 3: **Data Export**
    - Save the enriched dataset from Part 2, Task 3, in Parquet format, partitioned by category and transaction_date.
    - Save the store_id-level revenue insights (from Part 2, Task 1) in CSV format.

    
## Deliverables
- Your solution should be uploaded and submitted using a *GITHUB REPO* as base
- Your solution should follow good project architecture.
- You should provide a github repository and include as collaborators the following GH handlers (ccarvalheira JulianSaboridoP pformoso-deus-ai Danny-DEUS) 

- **Output files**:
    - Enriched dataset in Parquet format.
    - Revenue insights in CSV format.
    - Create a documentation file in Markdown for documenting your project including:
        - Your approach to the problem.
        - Any assumptions or decisions made.
        - Steps to run the code and reproduce results.

- **Submission Guidelines**
    - Once you finish the challenge ping us via the contact mail we provided attaching the repository link.

## Evaluation Criteria
- Correctness and completeness of the solution.
- Efficient use of PySpark for data processing.
- Code quality (readability, comments, modularity, good practices).
- Adherence to the deliverable format and instructions.
- Handling of edge cases and data validation.
