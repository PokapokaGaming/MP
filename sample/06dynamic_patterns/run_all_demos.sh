#!/bin/bash
# run_all_demos.sh - 全パターンのデモを実行

cd "$(dirname "$0")"

echo "Compiling all patterns..."
erlc pattern1_same_party.erl
erlc pattern2_cross_party_connect.erl
erlc pattern3_new_party.erl

echo ""
echo "========================================"
echo "Pattern 1: Same Party Node Addition"
echo "========================================"
erl -noshell -eval "pattern1_same_party:demo(), halt()."

echo ""
echo "========================================"
echo "Pattern 2a: Cross-Party (both periodic)"
echo "========================================"
erl -noshell -eval "pattern2_cross_party_connect:demo_2a(), halt()."

echo ""
echo "========================================"
echo "Pattern 2b: Cross-Party (with any_party)"
echo "========================================"
erl -noshell -eval "pattern2_cross_party_connect:demo_2b(), halt()."

echo ""
echo "========================================"
echo "Pattern 3a: New Party (any_party)"
echo "========================================"
erl -noshell -eval "pattern3_new_party:demo_3a(), halt()."

echo ""
echo "========================================"
echo "Pattern 3b: New Party (periodic)"
echo "========================================"
erl -noshell -eval "pattern3_new_party:demo_3b(), halt()."

echo ""
echo "========================================"
echo "All demos complete!"
echo "========================================"
