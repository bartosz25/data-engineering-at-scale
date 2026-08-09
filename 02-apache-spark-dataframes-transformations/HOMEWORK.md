# Homework

## Exercise 1
  You are given a Delta Lake table called _players_ with at least three columns: player, club, and wage.                                                                     
                                                                                                                                                                           
  Your task is to build a transformation pipeline that:                                                                                                                    
  1. Reads the _players_ table
  2. Keeps only the three columns listed above
  3. Filters out any player earning $1,000 or more
  4. Writes the remaining rows into a new Delta Lake table called _cheap_players_

  Do not write any code. Instead, write a short explanation covering:
- Which API would you use for data transformation part — PySpark SQL (a SQL query string passed to `spark.sql(...)`) or the PySpark Python API (`DataFrame` methods like `.select()`, `.filter()`)?
Justify your choice.
- Is there any runtime difference between the two approaches? Think about what happens under the hood before you answer.

## Exercise 2
Pick any example from today's lesson that writes a Delta Lake table.

Run it, then intentionally modify the data in two separate steps:
1. Insert at least one new row into the table
2. Update at least one existing row

After each step, open the `_delta_log/` directory inside your table's output folder and record what you find there. Your written analysis should answer:
- What files appear in _`_delta_log/` after the initial write?
- What changes in _`_delta_log/` after the insert? After the update?
- What information is stored in those files, and what does it tell you about how Delta Lake tracks changes?
