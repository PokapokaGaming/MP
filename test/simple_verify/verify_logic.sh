#!/bin/bash
# Verify counter and doubler logic from log

echo "=== Counter Values (first 20) ==="
grep "DATA\[counter_1_c\]" new_log2.txt | head -20

echo ""
echo "=== Doubler Values (first 20) ==="
grep "DATA\[doubler_2_doubled\]" new_log2.txt | head -20

echo ""
echo "=== Verification: doubler = counter * 2 ==="
for i in {1..10}; do
    counter=$(grep "DATA\[counter_1_c\]=$i$" new_log2.txt | head -1 | sed 's/.*=//')
    doubled=$((i * 2))
    actual=$(grep "DATA\[doubler_2_doubled\]=$doubled$" new_log2.txt | head -1 | sed 's/.*=//')
    if [ "$actual" = "$doubled" ]; then
        echo "✓ counter=$i → doubled=$doubled (correct)"
    else
        echo "✗ counter=$i → expected=$doubled, actual=$actual (ERROR)"
    fi
done
