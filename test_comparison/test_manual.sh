#!/bin/bash
cd "$(dirname "$0")"

cp main_new.erl main.erl
cat >> main.erl << 'EOF'
key_input() -> 0.
EOF

erlc main.erl

# Create a test that manually sends messages
cat > test_manual.erl << 'EOF'
-module(test_manual).
-export([run/0]).

run() ->
    main:start(),
    timer:sleep(100),
    % Check which processes are registered
    io:format("Registered processes: ~p~n", [registered()]),
    % Manually send sync pulse to party leader (main_data)
    io:format("Sending {main, 0} to main_data~n"),
    main_data ! {main, 0},
    timer:sleep(500),
    io:format("Done~n"),
    halt(0).
EOF

erlc test_manual.erl
erl -noshell -s test_manual run
