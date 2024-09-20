# Spark Mini Project: Automobile Post-Sales Report

This project redesigns the Hadoop auto post-sales report using Apache Spark. It processes a dataset of vehicle incident history to produce a report of the total number of accidents per make and year of the car.

## Project Overview

The project analyzes a dataset containing various incidents (initial sales, accidents, repairs) for vehicles. The goal is to write a Spark job that filters and processes this data to count the number of accidents for each make and year of the cars.

## Prerequisites

- Apache Spark
- Python 3.x
- Hadoop environment (Hortonworks Hadoop Sandbox recommended)

## Setup

1. Set up your Hadoop environment. If you don't have one, we recommend using Hortonworks Hadoop Sandbox.
2. Add the `data.csv` file to your Hadoop file system where it can be accessed by the Spark job.

## Project Structure

- `autoinc_spark.py`: Main Python script containing the Spark job
- `run_spark_job.sh`: Shell script to submit the Spark job
- `data.csv`: Input dataset (to be placed in your Hadoop file system)

## Running the Project

1. Ensure your Hadoop environment is set up and running.
2. Place the `data.csv` file in the appropriate location in your Hadoop file system.
3. Run the shell script to submit the Spark job:

   ```
   ./run_spark_job.sh
   ```

## Implementation Details

The Spark job consists of the following main steps:

1. Filter out accident incidents and propagate make and year information.
2. Count the number of accidents per make and year of the car.
3. Save the results to HDFS as a text file.

Key functions to implement:

- `extract_vin_key_value()`
- `populate_make()`
- `extract_make_key_value()`

## Output

The output file will be saved in HDFS and should look similar to this:

```
Nissan-2003,1
BMW-2008,10
MERCEDES-2013,2
```

## Verifying Results

To verify the results:

1. Check the Spark job execution log for any errors.
2. Use Hadoop commands to view the output file in HDFS:
   ```
   hdfs dfs -cat /path/to/output/file
   ```

## Troubleshooting

If you encounter any issues:

1. Check the Spark application logs for error messages.
2. Ensure all dependencies are correctly installed and configured.
3. Verify that the input data file is correctly placed and accessible in your Hadoop file system.

## Contributing

Contributions to improve the project are welcome. Please feel free to submit a pull request or open an issue for discussion.

## License

