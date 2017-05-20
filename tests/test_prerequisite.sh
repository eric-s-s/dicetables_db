#!/usr/bin/env bash
python create_numbers_on_connection_tests.py

# NOTE - THIS IS TO WORK ON MY MACHINE.  ANYONE USING MONGO_DB SHOULD COMMENT OUT THIS LINE AND USE WHATEVER
# COMMAND WILL GET YOUR MONGO SERVER RUNNING.  (like "mongod" )
/c/Program\ Files/MongoDB/Server/3.4/bin/mongod.exe --dbpath "e:/work/data"
