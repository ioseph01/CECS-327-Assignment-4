#!/bin/bash

# ------------------------------------------------------------------ #
#  Configuration                                                       #
# ------------------------------------------------------------------ #

PYTHON=python3
CLI="$PYTHON cli.py"
BOOTSTRAP_PORT=5004
SLEEP_JOIN=2        # seconds to wait after each node joins
SLEEP_STAB=5        # seconds to let stabilization settle
SLEEP_CMD=1         # seconds between commands

# ------------------------------------------------------------------ #
#  Cleanup function                                                    #
# ------------------------------------------------------------------ #

cleanup() {
    echo ""
    echo "=== Shutting down all nodes ==="
    kill $NODE1_PID $NODE2_PID $NODE3_PID $NODE4_PID $NODE5_PID 2>/dev/null
    wait 2>/dev/null
    echo "Done."
}
trap cleanup EXIT

# ------------------------------------------------------------------ #
#  Step 1 — Launch nodes                                               #
# ------------------------------------------------------------------ #

echo "=== Starting Chord Ring ==="

echo "Starting bootstrap node 4 on port 5004..."
$PYTHON main.py --id 4 --port 5004 &
NODE1_PID=$!
sleep $SLEEP_JOIN

echo "Starting node 8 on port 5008..."
$PYTHON main.py --id 8 --port 5008 --bootstrap localhost:$BOOTSTRAP_PORT &
NODE2_PID=$!
sleep $SLEEP_JOIN

echo "Starting node 15 on port 5015..."
$PYTHON main.py --id 15 --port 5015 --bootstrap localhost:$BOOTSTRAP_PORT &
NODE3_PID=$!
sleep $SLEEP_JOIN

echo "Starting node 27 on port 5027..."
$PYTHON main.py --id 27 --port 5027 --bootstrap localhost:$BOOTSTRAP_PORT &
NODE4_PID=$!
sleep $SLEEP_JOIN

echo "Starting node 44 on port 5044..."
$PYTHON main.py --id 44 --port 5044 --bootstrap localhost:$BOOTSTRAP_PORT &
NODE5_PID=$!
sleep $SLEEP_JOIN

echo ""
echo "Waiting ${SLEEP_STAB}s for ring to stabilize..."
sleep $SLEEP_STAB

# ------------------------------------------------------------------ #
#  Step 2 — DFS operations                                             #
# ------------------------------------------------------------------ #

echo ""
echo "=== DFS Operations ==="

echo "--- touch music.txt ---"
$CLI --port 5004 tou