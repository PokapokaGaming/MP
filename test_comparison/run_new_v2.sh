#!/bin/bash
# Run new MPFRP compiled code with key_input implementation

cd "$(dirname "$0")"

# Compile Erlang (rename to match module name)
cp main_new.erl main.erl

# Add key_input implementation to main.erl
cat >> main.erl << 'EOF'

% Native function implementation for testing
key_input() -> 0.
EOF

erlc main.erl

# Create test script that sends sync pulses
cat > test_new.erl << 'EOF'
-module(test_new).
-export([run/0]).

run() ->
    main:start(),
    timer:sleep(100),
    % Send 5 sync pulses to party_main (60s period party)
    lists:foreach(fun(I) ->
        party_main ! I,
        timer:sleep(50)
    end, lists:seq(0, 4)),
    timer:sleep(100),
    init:stop().
EOF

# Compile and run test
erlc test_new.erl
erl -noshell -s test_new run 2>&1 | grep "NEW_DATA" | sort > new_output.txt

# Cleanup
rm -f main.erl main.beam test_new.erl test_new.beam

echo "New output captured to new_output.txt"
echo "Total lines: $(wc -l < new_output.txt)"
cat new_output.txt
