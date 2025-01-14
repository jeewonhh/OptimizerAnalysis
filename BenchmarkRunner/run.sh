#!/bin/bash

source .env

#python -c'import run; run.end_to_end_explain_run("tpch")'
python -c 'import run; run.end_to_end_run("tpch")'