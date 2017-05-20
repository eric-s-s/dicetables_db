##a project to make a big database of dicetables later to be used with a server

If you don't know what a dicetable is, see the README at [dice-tables](https://github.com/eric-s-s/dice-tables).

frontend.py currently broken. Highest level is insertandretrieve.py.  here's the API.
 
```python
import dicetables as dt
from dicetables_db.connections.sql_connection import SQLConnection
from dicetables_db.insertandretrieve import DiceTableInsertionAndRetrieval

connection = SQLConnection(':memory:', 'a_table_name')

in_out = DiceTableInsertionAndRetrieval(connection)

table = dt.DiceTable.new().add_die(dt.Die(6), 1000)

in_out.add_table(table)

in_out.find_nearest_table([(dt.Die(6), 999)]) # returns None

doc_id = in_out.find_nearest_table([(dt.Die(6), 1000)]) # returns id of table

in_out.get_table(doc_id) # returns copy of 'table' from serialized data

```

### current plan

since all Die reprs are unique, make a repr parser.  search db using strings.
When a table is requested:

- Parse the string to a list of [(Die, number), ...]
- Retrieve the nearest table from db
- Figure out difference to achieve target table
- Do adds of set size size until target table is reached
- Inserting each intermediate table into db for future use as you go
- Return requested table
- Lather, rinse, repeat