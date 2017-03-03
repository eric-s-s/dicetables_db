#!/usr/bin/env bash
/c/Program\ Files/MongoDB/Server/3.4/bin/mongod.exe --dbpath "e:/work/data"
python -m unittest discover tests
