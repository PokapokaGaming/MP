#!/bin/bash
# Test Pattern 3: in dummy あり × buggy codegen_new

echo "===== Pattern 3: with_dummy × buggy ====="

# Clean up
cd /Users/work/Documents/MP
make clean > /dev/null 2>&1
rm -f test/nextver_bug_verification/with_dummy_buggy/*.erl
rm -f test/nextver_bug_verification/with_dummy_buggy/*.beam

# Copy buggy version
cp src/codegen_new_buggy.ml src/codegen_new.ml

# Rebuild compiler
make > /dev/null 2>&1

# Compile XFRP to Erlang
./main.native -std -inst \
    test/nextver_bug_verification/with_dummy_buggy/main.xfrp \
    test/nextver_bug_verification/Counter_with_dummy.xfrp \
    test/nextver_bug_verification/Adder.xfrp \
    > test/nextver_bug_verification/with_dummy_buggy/main.erl 2>&1

# Compile Erlang
cd test/nextver_bug_verification/with_dummy_buggy
erlc main.erl 2>&1

# Run and capture log (use perl for timeout on macOS)
(erl -noshell -s main start & PID=$!; sleep 2; kill $PID 2>/dev/null) 2>&1 | tee log.txt

echo ""
echo "Log saved to: test/nextver_bug_verification/with_dummy_buggy/log.txt"
echo ""
