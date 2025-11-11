#!/bin/bash
set -e

cd erlang

# Copy files with correct names
cp main_inst_new.erl main_new.erl
cp main_inst_original.erl main_original.erl

echo "=== Compiling NEW version ==="
erlc main_new.erl 2>&1 | grep -v Warning || true

echo ""
echo "=== Running NEW version (3 seconds) ==="
timeout 3 erl -noshell -s main start 2>&1 | grep -E "(PARTY|DATA)" > new_output.txt || true
echo "First 30 lines of NEW output:"
head -30 new_output.txt

echo ""
echo "=== Compiling ORIGINAL version ==="
# Clean up first
rm -f main.beam
erlc main_original.erl 2>&1 | grep -v Warning || true

echo ""
echo "=== Running ORIGINAL version (3 seconds) ==="
timeout 3 erl -noshell -s main start 2>&1 | grep -E "(PARTY|ORIGINAL_DATA)" > original_output.txt || true
echo "First 30 lines of ORIGINAL output:"
head -30 original_output.txt

echo ""
echo "=== Comparison ==="
echo "NEW output lines: $(wc -l < new_output.txt | tr -d ' ')"
echo "ORIGINAL output lines: $(wc -l < original_output.txt | tr -d ' ')"
