#!/bin/bash
# Test Pattern 2: in dummy なし × fixed codegen_new

echo "===== Pattern 2: no_dummy × fixed ====="

# Clean up
cd /Users/work/Documents/MP
make clean > /dev/null 2>&1
rm -f test/nextver_bug_verification/no_dummy_fixed/*.erl
rm -f test/nextver_bug_verification/no_dummy_fixed/*.beam

# Copy fixed version
cp src/codegen_new_fixed.ml src/codegen_new.ml

# Rebuild compiler
make > /dev/null 2>&1

# Compile XFRP to Erlang
./main.native -std -inst \
    test/nextver_bug_verification/no_dummy_fixed/main.xfrp \
    test/nextver_bug_verification/Counter_no_dummy.xfrp \
    test/nextver_bug_verification/Adder.xfrp \
    > test/nextver_bug_verification/no_dummy_fixed/main.erl 2>&1

# Compile Erlang
cd test/nextver_bug_verification/no_dummy_fixed
erlc main.erl 2>&1

# Run and capture log (use perl for timeout on macOS)
(erl -noshell -s main start & PID=$!; sleep 2; kill $PID 2>/dev/null) 2>&1 | tee log.txt

echo ""
echo "Log saved to: test/nextver_bug_verification/no_dummy_fixed/log.txt"
echo ""
