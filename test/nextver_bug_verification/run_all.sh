#!/bin/bash
# Run all 4 patterns and generate comparison report

echo "======================================"
echo "NextVer Increment Bug Verification"
echo "Testing 4 patterns:"
echo "1. no_dummy × buggy"
echo "2. no_dummy × fixed"
echo "3. with_dummy × buggy"
echo "4. with_dummy × fixed"
echo "======================================"
echo ""

cd /Users/work/Documents/MP/test/nextver_bug_verification

# Run all patterns
chmod +x run_pattern1.sh run_pattern2.sh run_pattern3.sh run_pattern4.sh

./run_pattern1.sh
./run_pattern2.sh
./run_pattern3.sh
./run_pattern4.sh

# Generate report
echo "======================================"
echo "COMPARISON REPORT"
echo "======================================"
echo ""

echo "Pattern 1: no_dummy × buggy"
echo "----------------------------"
echo "NextVer increments:"
grep -c "NextVer" no_dummy_buggy/log.txt 2>/dev/null || echo "0"
echo "BUGGY messages:"
grep -c "BUGGY" no_dummy_buggy/log.txt 2>/dev/null || echo "0"
echo "DATA outputs:"
grep -c "DATA\[" no_dummy_buggy/log.txt 2>/dev/null || echo "0"
echo ""

echo "Pattern 2: no_dummy × fixed"
echo "----------------------------"
echo "NextVer increments:"
grep -c "NextVer" no_dummy_fixed/log.txt 2>/dev/null || echo "0"
echo "BUGGY messages:"
grep -c "BUGGY" no_dummy_fixed/log.txt 2>/dev/null || echo "0"
echo "DATA outputs:"
grep -c "DATA\[" no_dummy_fixed/log.txt 2>/dev/null || echo "0"
echo ""

echo "Pattern 3: with_dummy × buggy"
echo "------------------------------"
echo "NextVer increments:"
grep -c "NextVer" with_dummy_buggy/log.txt 2>/dev/null || echo "0"
echo "BUGGY messages:"
grep -c "BUGGY" with_dummy_buggy/log.txt 2>/dev/null || echo "0"
echo "DATA outputs:"
grep -c "DATA\[" with_dummy_buggy/log.txt 2>/dev/null || echo "0"
echo ""

echo "Pattern 4: with_dummy × fixed"
echo "------------------------------"
echo "NextVer increments:"
grep -c "NextVer" with_dummy_fixed/log.txt 2>/dev/null || echo "0"
echo "BUGGY messages:"
grep -c "BUGGY" with_dummy_fixed/log.txt 2>/dev/null || echo "0"
echo "DATA outputs:"
grep -c "DATA\[" with_dummy_fixed/log.txt 2>/dev/null || echo "0"
echo ""

echo "======================================"
echo "Detailed logs:"
echo "- no_dummy_buggy/log.txt"
echo "- no_dummy_fixed/log.txt"
echo "- with_dummy_buggy/log.txt"
echo "- with_dummy_fixed/log.txt"
echo "======================================"
