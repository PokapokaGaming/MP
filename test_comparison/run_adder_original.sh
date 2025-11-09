#!/bin/bash

# Create temporary directory for original compiler
mkdir -p /Users/work/Documents/MP/test_comparison/original_build

# Compile A+B=C test case with original compiler
cd /Users/work/Documents/MP/mpfrp-original
./main.native ../test_comparison/original_src/counter_a.xfrp ../test_comparison/original_src/counter_b.xfrp ../test_comparison/original_src/adder.xfrp -inst ../test_comparison/original_src/main_adder.xfrp > ../test_comparison/original_build/main.erl

# Compile the generated Erlang code
cd /Users/work/Documents/MP/test_comparison/original_build
erlc main.erl

# Run and capture output (get first 20 lines only)
erl -noshell -eval "
main:start(),
timer:sleep(1000),
halt().
" 2>&1 | head -50
