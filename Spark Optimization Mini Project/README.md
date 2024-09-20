# Query Optimization with PySpark

This project demonstrates query optimization techniques using PySpark. It includes two versions of a query that retrieves the number of answers for each question per month, along with a comparison of their execution times and query plans.

## Overview

The script `optimize.py` contains two implementations of the same query:

1. An initial, suboptimal version
2. An optimized version with improved performance

Both queries join data from questions and answers datasets, aggregate the number of answers per question per month, and display the results.

## Prerequisites

- Python 3.x
- PySpark
- Access to question and answer datasets (paths are configured in the script)

## File Structure

- `optimize.py`: Main script containing both query implementations
- `data/`: Directory containing input datasets
  - `answers/`: Answers dataset
  - `questions/`: Questions dataset

## Usage

1. Ensure you have PySpark installed and configured.
2. Update the `base_path` variable in the script if necessary.
3. Run the script:

```
python optimize.py
```

## Query Implementations

### Initial Query

The initial query performs the following steps:
1. Reads questions and answers datasets
2. Aggregates answers by month
3. Joins the aggregated answers with questions
4. Orders the results by question_id and month

### Optimized Query

The optimized query includes these improvements:
1. Repartitions DataFrames by `question_id` to minimize shuffle during join
2. Applies filtering and selects only necessary columns before aggregation
3. Optimizes the join operation

## Performance Comparison

The script outputs the execution time for both queries, allowing for easy comparison. It also displays the query plans, which can be analyzed for further optimization opportunities.

## Contributing

Contributions to improve the query optimization or expand the project are welcome. Please feel free to submit a pull request or open an issue for discussion.

## License

[Specify your license here]
