#!/bin/bash
cd "$(dirname "$0")"

cp main_new.erl main.erl
cat >> main.erl << 'EOF'
key_input() -> 0.
EOF

erlc main.erl 2>&1 | grep -v Warning

# Create trace test
cat > test_trace.erl << 'EOF'
-module(test_trace).
-export([run/0]).

run() ->
    io:format("Starting system...~n"),
    main:start(),
    timer:sleep(100),
    io:format("Sending {main, 0} to main_data~n"),
    main_data ! {main, 0},
    timer:sleep(1000),
    io:format("Done~n"),
    halt(0).
EOF

erlc test_trace.erl
erl -noshell -s test_trace run
