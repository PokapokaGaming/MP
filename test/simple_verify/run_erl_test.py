#!/usr/bin/env python3
import subprocess
import time
import signal
import sys

proc = subprocess.Popen(
    ["erl", "-noshell", "-pa", "."],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    cwd="erlang"
)

# Send commands
proc.stdin.write("main:start().\n")
proc.stdin.flush()
time.sleep(2)

# Terminate
proc.send_signal(signal.SIGTERM)
output, _ = proc.communicate(timeout=5)

print(output)
