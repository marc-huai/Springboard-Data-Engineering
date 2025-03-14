# Azure Key Vault and Azure Data Factory

## Why should one use Azure Key Vault when working in the Azure environment? What are the alternatives to using Azure Key Vault? What are the pros and cons of using Azure Key Vault?

Azure Key Vault is a secure way to store sensitive information like passwords, API keys, and encryption keys, keeping them safe and out of your code. It helps control access, works well with other Azure services, and provides a single place to manage secrets.

Instead of using Key Vault, some alternatives include storing secrets in environment variables or encrypted configuration files. The main benefits of Key Vault are strong security, easy access control, and built-in tracking of who uses the secrets. On the downside, retrieving secrets can take a little longer, frequent access might add costs, and it requires some setup to use properly. Still, it’s a great choice for keeping sensitive data safe in Azure.

## How do you achieve the loop functionality within an Azure Data Factory pipeline? Why would you need to use this functionality in a data pipeline?

Looping in Azure Data Factory is done with the **ForEach activity** to process multiple items one by one and the **Until activity** to repeat a task until a condition is met. This is useful when dealing with multiple files or automating tasks instead of setting them up manually.

## What are expressions in Azure Data Factory? How are they helpful when designing a data pipeline (please explain with an example)?

Expressions allow pipelines to use dynamic values instead of fixed ones. For example, instead of writing a file name manually, an expression can generate a new one each day using the current date. This helps automate pipelines and reduces manual updates.

## What are the pros and cons of parametrizing a dataset in an Azure Data Factory pipeline’s activity?

Parameterizing a dataset makes pipelines more flexible because they can handle different data sources without changes. This makes things easier to maintain, but it can also make debugging harder since values are always changing.

## What are the different supported file formats and compression codecs in Azure Data Factory? When will you use a Parquet file over an ORC file? Why would you choose an AVRO file format over a Parquet file format?

Azure Data Factory supports formats like **CSV, Parquet, Avro, and ORC**. Parquet is great for fast data analysis, ORC works well in Hive-based systems, and Avro is better for streaming and transferring data because of its simple format.
