#!/bin/bash

# ------------------------------------------------------------------ #
#  Configuration                                                       #
# ------------------------------------------------------------------ #

PYTHON=python3
CLI="$PYTHON cli.py"
BOOTSTRAP_PORT=5004
SLEEP_JOIN=2        # seconds to wait after each node joins
SLEEP_STAB=15        # seconds to let stabilization settle
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

$CLI --port 5004 stat music.txt   # just to trigger something
# then check successor chain manually:
for port in 5004 5008 5015 5027 5044; do
    echo -n " "
    python3 -c "from Server.client import send_message; r = send_message('localhost', $port, {'type':'get_pred'}); print(r['address'] + '-> ', end='')"
    python3 -c "print($port, end='')"
    python3 -c "from Server.client import send_message; r = send_message('localhost', $port, {'type':'get_succ'}); print('->' + r['address'])"
done

# ------------------------------------------------------------------ #
#  Step 2 — DFS operations                                             #
# ------------------------------------------------------------------ #

echo ""
echo "=== DFS Operations ==="

echo "--- touch music.txt ---"
$CLI --port 5004 touch music.txt
sleep $SLEEP_CMD

echo "--- touch test.txt ---"
$CLI --port 5004 touch test.txt
sleep $SLEEP_CMD

echo "--- stat music.txt ---"
$CLI --port 5004 stat music.txt
sleep $SLEEP_CMD

# create local files to append
echo "hello world" > tmp/page1.txt
echo "distributed file systems" > tmp/page2.txt
echo "chord ring routing" > tmp/page3.txt

echo "--- append page1.txt ---"
$CLI --port 5004 append music.txt tmp/page1.txt
sleep $SLEEP_CMD

echo "--- append page2.txt ---"
$CLI --port 5008 append music.txt tmp/page2.txt
sleep $SLEEP_CMD

echo "--- append page3.txt ---"
$CLI --port 5015 append music.txt tmp/page3.txt
sleep $SLEEP_CMD

echo "--- read music.txt ---"
$CLI --port 5004 read music.txt
sleep $SLEEP_CMD

echo "--- head music.txt 2 ---"
$CLI --port 5004 head music.txt 2
sleep $SLEEP_CMD

echo "--- tail music.txt 2 ---"
$CLI --port 5004 tail music.txt 2
sleep $SLEEP_CMD

echo "--- ls ---"
$CLI --port 5004 ls
sleep $SLEEP_CMD

echo "--- stat music.txt ---"
$CLI --port 5004 stat music.txt
sleep $SLEEP_CMD


# ------------------------------------------------------------------ #
#  Step 4 — Replication + Paxos                                        #
# ------------------------------------------------------------------ #

echo ""
echo "=== Replication and Paxos ==="

echo "--- ls ---"
$CLI --port 5004 ls
sleep $SLEEP_CMD

echo "--- touch replicated.txt ---"
$CLI --port 5004 touch replicated.txt
sleep $SLEEP_CMD

echo "--- ls ---"
$CLI --port 5004 ls
sleep $SLEEP_CMD

echo "--- stat replicated.txt (shows replica nodes) ---"
$CLI --port 5004 stat replicated.txt
sleep $SLEEP_CMD

echo "Creating content for replicated file..."
echo "replicated content line" > tmp/rep_input.txt

echo "--- append to replicated.txt (triggers Paxos) ---"
$CLI --port 5004 append replicated.txt tmp/rep_input.txt
sleep $SLEEP_CMD


# ------------------------------------------------------------------ #
#  Step 3 — Distributed Sort                                           #
# ------------------------------------------------------------------ #

echo ""
echo "=== Distributed Sort ==="

# create input file with 100+ records
python3 -c "
import random
records = [(f'{random.randint(0,9999):04d}', f'value{i}') for i in range(50)]
with open('tmp/sort_input.txt', 'w') as f:
    for k,v in records:
        f.write(f'{k},{v}\n')
print('Generated 50 records in tmp/sort_input.txt')
"

echo "--- touch input.csv ---"
$CLI --port 5004 touch input.csv
sleep $SLEEP_CMD

echo "--- append sort_input.txt ---"
$CLI --port 5004 append input.csv tmp/sort_input.txt
sleep $SLEEP_CMD

echo "--- sort_file input.csv output.csv ---"
$CLI --port 5004 sort input.csv output.csv
sleep $SLEEP_CMD

echo "--- verify first 5 lines of output.csv ---"
$CLI --port 5004 head output.csv 5
sleep $SLEEP_CMD

echo "--- verify last 5 lines of output.csv ---"
$CLI --port 5004 tail output.csv 5
sleep $SLEEP_CMD


# ------------------------------------------------------------------ #
#  Step 5 — Failure Scenario                                           #
# ------------------------------------------------------------------ #

echo ""
echo "=== Failure Scenario ==="

echo "--- read replicated.txt before crash ---"
$CLI --port 5004 read replicated.txt
sleep $SLEEP_CMD

echo "Crashing node 44 (killing process $NODE5_PID)..."
kill $NODE5_PID 2>/dev/null
NODE5_PID=""
sleep $SLEEP_CMD

echo "--- append after node 44 crash (Paxos majority with 2/3) ---"
echo "after crash content" > tmp/after_crash.txt
$CLI --port 5004 append replicated.txt tmp/after_crash.txt
sleep $SLEEP_CMD

echo "--- read replicated.txt after crash ---"
$CLI --port 5004 read replicated.txt
sleep $SLEEP_CMD

echo "--- ls after crash ---"
$CLI --port 5004 ls
sleep $SLEEP_CMD

# ------------------------------------------------------------------ #
#  Step 6 — Delete                                                     #
# ------------------------------------------------------------------ #

echo ""
echo "=== Cleanup ==="

echo "--- delete music.txt ---"
$CLI --port 5004 delete music.txt
sleep $SLEEP_CMD

echo "--- ls after delete ---"
$CLI --port 5004 ls
sleep $SLEEP_CMD

echo "--- read deleted file ---"
$CLI --port 5004 read music.txt
sleep $SLEEP_CMD

echo "--- delete input.csv ---"
$CLI --port 5004 delete input.csv
sleep $SLEEP_CMD

echo "--- read deleted file ---"
$CLI --port 5004 delete input.csv
sleep $SLEEP_CMD

echo ""
echo "=== Demo Complete ==="