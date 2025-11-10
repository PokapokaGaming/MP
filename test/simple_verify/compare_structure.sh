#!/bin/bash
# Compare generated code structure with mpfrp-original reference

echo "=== MP/codegen_new.ml Generated Code ==="
echo "File: main.erl"
wc -l main.erl
echo ""

echo "Functions exported:"
grep "^-export" main.erl
echo ""

echo "Key actors:"
grep "^[a-z_]*(" main.erl | head -10
echo ""

echo "=== Code Pattern Check ==="
echo "✓ Party actor (p/1):" $(grep -c "^p(Interval)" main.erl)
echo "✓ Module actors (counter_1, doubler_2):" $(grep -c "^counter_1\|^doubler_2" main.erl | head -2)
echo "✓ Computation nodes:" $(grep -c "^counter_1_c\|^doubler_2_doubled" main.erl | head -2)
echo "✓ Input node actors:" $(grep -c "^doubler_2_count" main.erl)
echo "✓ Request nodes:" $(grep -c "request_node" main.erl)
echo ""

echo "=== Buffer Processing Patterns ==="
echo "Buffer foldl count:" $(grep -c "lists:foldl.*Buffer" main.erl)
echo "Ver_buffer processing:" $(grep -c "Sorted_ver_buf" main.erl)
echo "In_buffer processing:" $(grep -c "Sorted_in_buf" main.erl)
echo "ReqBuffer processing:" $(grep -c "Sorted_req_buf" main.erl)
