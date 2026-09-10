# Homework

## Exercise 1
The current orchestration in the [orchestrate.sh](orchestrate.sh) assigns exactly one 
file to each runner. Adapt this behavior to a more concurrent execution:

- instead of assigning one file, assign at least two files for each runner

## Exercise 2
The current orchestration in the [orchestrate.sh](orchestrate.sh) executes
workers separately. Unfortunately, the requirements have changed you need now to combine 
the results from these partial executions and find the worker who processed the most valuable
orders.

Use the existing _total revenue_ metric to get the file with the most valuable orders.

The aggregation logic should work correctly regardless of how many runners were used or how many 
files each one handled.
