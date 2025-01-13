#!/bin/bash

source .env

#python3 main.py -o og -b tpch -e 1
#python3 main.py -o cd -b tpch -e 1
#python3 main.py -o ts -b tpch -e 1
#python3 main.py -o ds -b tpch -e 1
#python3 main.py -o ds_simplified -b tpch -e 1

python -c'import run; run.union_of_differentiating_queries("tpch")'
