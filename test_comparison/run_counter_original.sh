#!/bin/bash

# Compile Counter/Doubler test case with mpfrp-original
cd /Users/work/Documents/MP/mpfrp-original
./main.native ../test_comparison/original_src/counter.xfrp ../test_comparison/original_src/doubler.xfrp -inst ../test_comparison/original_src/main_counter.xfrp -std > ../test_comparison/main.erl

# Compile the generated Erlang code
cd /Users/work/Documents/MP/test_comparison
erlc main.erl

# Run and capture output (get first 10 lines only)
erl -noshell -eval "
main:start(),
timer:sleep(1000),
halt().
" 2>&1 | grep "ORIGINAL_DATA" | head -10
