#!/bin/bash
# Debug new compiler execution

cd "$(dirname "$0")"

cp main_new.erl main.erl

# Add key_input and debug logging
cat >> main.erl << 'EOF'

% Native function implementation
key_input() -> 
    io:format("DEBUG: key_input() called~n", []),
    0.
EOF

erlc main.erl

# Create test with more verbose output
cat > test_debug.erl << 'EOF'
-module(test_debug).
-export([run/0]).

run() ->
    io:format("DEBUG: Starting system...~n", []),
    main:start(),
    timer:sleep(200),
    
    io:format("DEBUG: Sending sync_pulse 0 to party_main~n", []),
    party_main ! 0,
    timer:sleep(200),
    
    io:format("DEBUG: Sending sync_pulse 1 to party_main~n", []),
    party_main ! 1,
    timer:sleep(200),
    
    io:format("DEBUG: Done~n", []),
    init:stop().
EOF

erlc test_debug.erl
erl -noshell -s test_debug run 2>&1

rm -f main.erl main.beam test_debug.erl test_debug.beam
