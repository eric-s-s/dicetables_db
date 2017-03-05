##a project to make a big database of dicetables later to be used with a server

first start serving with "mongod" command.

```
>>> import databaseinterface as dbi

>>> import dicetables as dt

>>> conn = dbi.MongoDBConnection('<db_name>', '<collection_name>')

>>> interface = dbi.ConnecitonCommandInterface(conn)

>>> interface.add_table(dt.DiceTable.new().add_die(dt.Die(6), 3)

>>> id_string = interface.find_nearest_table([(dt.Die(3), 3), (dt.Die(6), 2)]

>>> 3d6 = interface.get_table(id_string)
```
