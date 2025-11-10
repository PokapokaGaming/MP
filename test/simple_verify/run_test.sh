#!/bin/bash
cd erlang
erl -noshell -eval '
main:start(),
timer:sleep(2000),
init:stop().
' 2>&1
