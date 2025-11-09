#!/bin/bash

# Create temporary directory for new compiler
mkdir -p /Users/work/Documents/MP/test_comparison/new_build

# Compile Counter/Doubler test case with new compiler
cd /Users/work/Documents/MP
./main.native sample/03dynamic/counter.xfrp sample/03dynamic/doubler.xfrp -inst sample/03dynamic/main_counter.xfrp -std > test_comparison/new_build/main.erl

# Compile the generated Erlang code
cd /Users/work/Documents/MP/test_comparison/new_build
erlc main.erl

# Run and capture output (get first 10 lines only)
erl -noshell -eval "
main:start(),
timer:sleep(1000),
halt().
" 2>&1 | grep "NEW_DATA" | head -10
