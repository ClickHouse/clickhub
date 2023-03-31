#!/bin/bash

NUMBER_OF_WORKERS=${1:-1}

for i in $(seq $NUMBER_OF_WORKERS)
do
  nohup python3 clickhub.py start_worker > worker-${i}.out 2>&1 &
done
