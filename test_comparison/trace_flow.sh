#!/bin/bash
cd "$(dirname "$0")"

cp main_new.erl main.erl

# Add logging to party_actor
sed -i.bak '13 a\
    io:format("PARTY[~p] sending {~p, ~p} to ~p~n", [PartyName, PartyName, Ver, Leader]),' main.erl

# Add logging to module actor Ver_buffer processing
sed -i.bak '26 a\
                                io:format("SERVER_MODULE received {~p, ~p}, sending to downstream~n", [Party, Ver]),' main.erl

# Add logging to client module actor
sed -i.bak '68 a\
                                io:format("CLIENT_MODULE[~p] received {~p, ~p}~n", [self(), Party, Ver]),' main.erl

cat >> main.erl << 'EOF'
key_input() -> 0.
EOF

erlc main.erl 2>&1 | grep -v Warning

# Create test
cat > test_trace.erl << 'EOF'
-module(test_trace).
-export([run/0]).

run() ->
    main:start(),
    timer:sleep(100),
    main_data ! {main, 0},
    timer:sleep(500),
    halt(0).
EOF

erlc test_trace.erl
erl -noshell -s test_trace run
