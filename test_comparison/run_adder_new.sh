#!/bin/bash

# Create temporary directory for new compiler
mkdir -p /Users/work/Documents/MP/test_comparison/new_build

# Compile A+B=C test case with new compiler
cd /Users/work/Documents/MP
./main.native sample/03dynamic/counter_a.xfrp sample/03dynamic/counter_b.xfrp sample/03dynamic/adder.xfrp -inst sample/03dynamic/main_adder.xfrp -std > test_comparison/new_build/main.erl

# Compile the generated Erlang code
cd /Users/work/Documents/MP/test_comparison/new_build
erlc main.erl

# Run and capture output (run for 2 seconds to see multiple sync pulses)
erl -noshell -eval "
main:start(),
timer:sleep(2000),
halt().
" 2>&1 | head -120
