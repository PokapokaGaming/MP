#!/bin/bash
# Debug new MPFRP flow

cd "$(dirname "$0")"

cp main_new.erl main.erl
cat >> main.erl << 'EOF'

key_input() -> 0.
EOF

erlc main.erl

# Create debug test script
cat > test_debug.erl << 'EOF'
-module(test_debug).
-export([run/0]).

run() ->
    io:format("Starting main...~n"),
    main:start(),
    timer:sleep(100),
    io:format("Sending sync pulses to party_main...~n"),
    lists:foreach(fun(I) ->
        io:format("Sending pulse ~p to party_main~n", [I]),
        party_main ! I,
        timer:sleep(100)
    end, lists:seq(0, 4)),
    timer:sleep(500),
    io:format("Test complete~n"),
    halt(0).
EOF

erlc test_debug.erl
erl -noshell -s test_debug run
