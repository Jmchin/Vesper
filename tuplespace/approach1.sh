#!/bin/bash

# approach1.sh
# automate setup for approach 1

# PYTHON="/usr/bin/python3"
# RUBY="/usr/bin/ruby"


# bring the recovery server online

./start_recovery_service.sh && kill $! &
sleep .2
./start_name_service.sh
