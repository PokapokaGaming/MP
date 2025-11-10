#!/bin/bash
cd erlang
erl <<EOF
c(main).
c(test_runner).
test_runner:run().
EOF
