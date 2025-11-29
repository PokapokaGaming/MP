#!/bin/bash
# run_demo.sh - 動的カウンターデモの実行スクリプト

cd "$(dirname "$0")"

echo "=== MPFRP Dynamic Counter Demo ==="
echo ""
echo "Starting Erlang shell..."
echo "In the Erlang shell, run: main:demo()."
echo ""

erl -noshell -eval "
    c(main),
    main:demo(),
    timer:sleep(1000),
    halt().
" 2>&1
