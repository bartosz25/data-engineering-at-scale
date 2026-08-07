# Homework

## Exercise 1
The current orchestration assigns exactly one file to each runner. Generalise this so the orchestrator accepts a `--files-per-runner` argument and splits the full list of  CSV files into batches of that size, launching one runner per batch.

## Exercise 2
Once all runners have finished, add an aggregation step that combines their individual outputs into a single global summary containing:

- total rows processed across all runners
- average order amount across all rows
- minimum order amount across all rows
- maximum order amount across all rows

The aggregation logic should work correctly regardless of how many runners were used or how many files each one handled.
