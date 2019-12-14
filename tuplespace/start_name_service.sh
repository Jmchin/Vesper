#!/usr/bin/bash

# start up the naming server and its tuplespace/adapter pair

# prog1 & prog2 && kill $!

./tuplespace.rb -c nameserver.yaml & ./adapter.rb -c nameserver.yaml && kill $! &

sleep .2
./nameserver.py 224.0.0.1 54322
