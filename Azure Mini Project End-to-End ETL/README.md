# Azure Synapse ETL Pipeline Project

## Overview
This project demonstrates how to deploy an end-to-end ETL solution using Azure Synapse Analytics. The solution involves loading, transforming, and ingesting data into a dedicated SQL pool using Azure Synapse Pipelines.

## Key Features
- Provisioning an Azure Synapse workspace
- Extracting data from Azure Data Lake Storage Gen2
- Transforming data using Synapse Data Flows
- Loading data into a dedicated SQL pool
- Automating ETL workflows with Azure Synapse Pipelines
- Monitoring and debugging data pipeline execution

## Prerequisites
Before starting, ensure the following:
- An Azure subscription with admin-level access
- Azure Synapse Analytics workspace with:
  - Azure Data Lake Storage Gen2 (ADLS)
  - Dedicated SQL Pool
- Azure Cloud Shell access (PowerShell)
- Git installed for cloning the setup scripts

## Project Setup

### 1. Provision an Azure Synapse Workspace
1. Sign in to the [Azure Portal](https://portal.azure.com).
2. Open Azure Cloud Shell and select PowerShell.
3. Clone the setup repository:
   ```sh
   rm -r dp-203 -f 
   git clone https://github.com/MicrosoftLearning/dp-203-azure-data-engineer dp-203
   ```
4. Navigate to the lab folder:
   ```sh
   cd dp-203/Allfiles/labs/10 
   ```
5. Run the setup script:
   ```sh
   ./setup.ps1
   ```
6. Provide a password for your Synapse SQL Pool when prompted.
7. Wait approximately ten minutes for the deployment to complete.

## Data Pipeline Implementation

### 2. Explore Data Sources
- The source data is stored in Azure Data Lake Storage Gen2 as a CSV file.
- The destination is a dedicated SQL pool table in Azure Synapse Analytics.

#### Steps to Locate Data:
1. In Azure Synapse Studio, navigate to the **Data Page** → **Linked Tab**.
2. Locate the Azure Data Lake Storage Gen2 account.
3. Browse to `files/data/` and locate `Product.csv`.
4. Preview the file to confirm the presence of product data.

### 3. Implement the ETL Pipeline
1. Create a new pipeline in Synapse Studio.
2. Add a Data Flow to the pipeline.
3. Configure the Data Flow:
   - **Source:** Read `Product.csv` from Azure Data Lake.
   - **Lookup:** Match records with existing SQL pool data.
   - **Alter Row:** Define Insert and Upsert conditions.
   - **Sink:** Write transformed data to Synapse SQL Pool.

#### Data Transformation Rules
| Action | Condition |
|--------|-------------|
| **Insert** | If Product does not exist in SQL |
| **Upsert** | If Product exists, update details |

### 4. Debug and Run the Pipeline
1. Enable Debug Mode in Synapse Studio.
2. Preview the data flow to validate transformations.
3. Publish the pipeline.
4. Trigger the pipeline execution.

## Monitoring Pipeline Execution
- Navigate to **Monitor** → **Pipeline Runs**.
- Check execution logs and performance metrics.
- Query `dbo.DimProduct` table to verify loaded data.

## Cleanup Resources
To avoid incurring unnecessary costs, delete the deployed resources:
```sh
# Navigate to Azure Resource Groups
# Find the created resource group (e.g., dp203-xxxxx)
# Select "Delete Resource Group" and confirm deletion
```

## References
- [Azure Synapse Analytics Documentation](https://learn.microsoft.com/azure/synapse-analytics/)
- [Azure Data Factory Data Flows](https://learn.microsoft.com/azure/data-factory/concepts-data-flow-overview)

## Conclusion
This project showcases the capabilities of Azure Synapse Analytics for building scalable ETL pipelines using cloud-based data engineering solutions.
