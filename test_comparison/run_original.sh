#!/bin/bash
# Run original MPFRP compiled code

cd "$(dirname "$0")"

# Compile Erlang (rename to match module name)
cp main_original.erl main.erl
erlc main.erl

# Create test script that sends sync pulses
cat > test_original.erl << 'EOF'
-module(test_original).
-export([run/0]).

run() ->
    main:start(),
    timer:sleep(100),
    % Send 5 sync pulses to party p
    lists:foreach(fun(I) ->
        p ! I,
        timer:sleep(50)
    end, lists:seq(0, 4)),
    timer:sleep(100),
    init:stop().
EOF

# Compile and run test
erlc test_original.erl
erl -noshell -s test_original run 2>&1 | grep "ORIGINAL_DATA" | sort > original_output.txt

# Cleanup
rm -f main.erl main.beam test_original.erl test_original.beam

echo "Original output captured to original_output.txt"
echo "Total lines: $(wc -l < original_output.txt)"
cat original_output.txt | head -20
