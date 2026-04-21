# demo.py
from config import SLEEP_JOIN, SLEEP_STAB
from Server.client import send_message
import subprocess
import sys
import time
import os

PYTHON = sys.executable
BOOTSTRAP_PORT = 5004
SLEEP_CMD = 1
PROCESSES = []

# ------------------------------------------------------------------ #
#  helpers                                                             #
# ------------------------------------------------------------------ #

def cli(port, *args):
    cmd = [PYTHON, "cli.py", "--port", str(port)] + list(args)
    result = subprocess.run(cmd, capture_output=True, text=True)
    output = result.stdout.strip() or result.stderr.strip()
    print(output)
    return output

def separator(title):
    print(f"\n{'='*10} {title} {'='*10}")

def start_nodes():
    separator("Starting Chord Ring")

    p = subprocess.Popen([PYTHON, "main.py", "--id", "4", "--port", "5004"])
    PROCESSES.append(p)
    print(f"Started bootstrap node 4 on port 5004 (pid={p.pid})")
    time.sleep(SLEEP_JOIN)

    for node_id, port in [(8, 5008), (15, 5015), (27, 5027), (44, 5044)]:
        p = subprocess.Popen([
            PYTHON, "main.py",
            "--id", str(node_id),
            "--port", str(port),
            "--bootstrap", f"localhost:{BOOTSTRAP_PORT}"
        ])
        PROCESSES.append(p)
        print(f"Started node {node_id} on port {port} (pid={p.pid})")
        time.sleep(SLEEP_JOIN)

    print(f"\nWaiting {SLEEP_STAB}s for ring to stabilize...")
    time.sleep(SLEEP_STAB)
    print("Ring ready.")

def check_ring():
    separator("Ring Structure")
    for port in [5004, 5008, 5015, 5027, 5044]:
        pred = send_message("localhost", port, {"type": "get_pred"})
        succ = send_message("localhost", port, {"type": "get_succ"})
        pred_addr = pred.get("address", "None")
        succ_addr = succ.get("address", "None")
        self_id = succ.get("self_id", "?")
        print(f"  node {self_id:>3} | pred={pred_addr} -> {port} -> succ={succ_addr}")

def write_tmp(filename, contents):
    os.makedirs("tmp", exist_ok=True)
    path = f"tmp/{filename}"
    with open(path, "w") as f:
        f.write(contents)
    return path

def stop_nodes():
    separator("Shutting down all nodes")
    for p in PROCESSES:
        try:
            p.terminate()
            p.wait(timeout=3)
        except Exception:
            p.kill()
    print("Done.")

# ------------------------------------------------------------------ #
#  demo steps                                                          #
# ------------------------------------------------------------------ #

def demo_dfs():
    separator("DFS Operations")

    print("--- touch music.txt ---")
    cli(5004, "touch", "music.txt")
    time.sleep(SLEEP_CMD)

    print("--- touch test.txt ---")
    cli(5004, "touch", "test.txt")
    time.sleep(SLEEP_CMD)

    print("--- stat music.txt ---")
    cli(5004, "stat", "music.txt")
    time.sleep(SLEEP_CMD)

    write_tmp("page1.txt", "hello world\n")
    write_tmp("page2.txt", "distributed file systems\n")
    write_tmp("page3.txt", "chord ring routing\n")

    print("--- append page1.txt ---")
    cli(5004, "append", "music.txt", "tmp/page1.txt")
    time.sleep(SLEEP_CMD)

    print("--- append page2.txt ---")
    cli(5008, "append", "music.txt", "tmp/page2.txt")
    time.sleep(SLEEP_CMD)

    print("--- append page3.txt ---")
    cli(5015, "append", "music.txt", "tmp/page3.txt")
    time.sleep(SLEEP_CMD)

    print("--- read music.txt ---")
    cli(5004, "read", "music.txt")
    time.sleep(SLEEP_CMD)

    print("--- head music.txt 2 ---")
    cli(5004, "head", "music.txt", "2")
    time.sleep(SLEEP_CMD)

    print("--- tail music.txt 2 ---")
    cli(5004, "tail", "music.txt", "2")
    time.sleep(SLEEP_CMD)

    print("--- ls ---")
    cli(5004, "ls")
    time.sleep(SLEEP_CMD)

    print("--- stat music.txt ---")
    cli(5004, "stat", "music.txt")
    time.sleep(SLEEP_CMD)

def demo_replication_paxos():
    separator("Replication and Paxos")

    print("--- ls ---")
    cli(5004, "ls")
    time.sleep(SLEEP_CMD)

    print("--- touch replicated.txt ---")
    cli(5004, "touch", "replicated.txt")
    time.sleep(SLEEP_CMD)

    print("--- ls ---")
    cli(5004, "ls")
    time.sleep(SLEEP_CMD)

    print("--- stat replicated.txt (shows replica nodes) ---")
    cli(5004, "stat", "replicated.txt")
    time.sleep(SLEEP_CMD)

    write_tmp("rep_input.txt", "replicated content line\n")

    print("--- append to replicated.txt (triggers Paxos) ---")
    cli(5004, "append", "replicated.txt", "tmp/rep_input.txt")
    time.sleep(SLEEP_CMD)

def demo_sort():
    separator("Distributed Sort")

    import random
    os.makedirs("tmp", exist_ok=True)
    records = [(f"{random.randint(0, 9999):04d}", f"value{i}") for i in range(100)]
    with open("tmp/sort_input.txt", "w") as f:
        for k, v in records:
            f.write(f"{k},{v}\n")
    print("Generated 100 records in tmp/sort_input.txt")

    print("--- touch input.csv ---")
    cli(5004, "touch", "input.csv")
    time.sleep(SLEEP_CMD)

    print("--- append sort_input.txt ---")
    cli(5004, "append", "input.csv", "tmp/sort_input.txt")
    time.sleep(SLEEP_CMD)

    print("--- sort_file input.csv output.csv ---")
    cli(5004, "sort", "input.csv", "output.csv")
    time.sleep(SLEEP_CMD)

    print("--- verify first 5 lines of output.csv ---")
    cli(5004, "head", "output.csv", "5")
    time.sleep(SLEEP_CMD)

    print("--- verify last 5 lines of output.csv ---")
    cli(5004, "tail", "output.csv", "5")
    time.sleep(SLEEP_CMD)

def demo_failure():
    separator("Failure Scenario")

    print("--- read replicated.txt before crash ---")
    cli(5004, "read", "replicated.txt")
    cli(5004, "stat", "replicated.txt")

    
    for port in [5004, 5008, 5015, 5027, 5044]:
        pred = send_message("localhost", port, {"type": "get_pred"})
        succ = send_message("localhost", port, {"type": "get_succ"})
        pred_addr = pred.get("address", "None")
        succ_addr = succ.get("address", "None")
        self_id = succ.get("self_id", "?")
        print(f"  node {self_id:>3} | pred={pred_addr} -> {port} -> succ={succ_addr}")
        
    time.sleep(SLEEP_CMD)

    crashed = PROCESSES[-1]
    print(f"Crashing node 44 (pid={crashed.pid})...")
    crashed.terminate()
    crashed.wait(timeout=3)
    print("Waiting for ring to stabilize...")
    time.sleep(SLEEP_STAB)

    for port in [5004, 5008, 5015, 5027, 5044]:
        pred = send_message("localhost", port, {"type": "get_pred"})
        succ = send_message("localhost", port, {"type": "get_succ"})
        pred_addr = pred.get("address", "None")
        succ_addr = succ.get("address", "None")
        self_id = succ.get("self_id", "?")
        print(f"  node {self_id:>3} | pred={pred_addr} -> {port} -> succ={succ_addr}")

    write_tmp("after_crash.txt", "after crash content\n")
    

    print("--- append after node 44 crash ---")
    cli(5004, "append", "replicated.txt", "tmp/after_crash.txt")
    time.sleep(SLEEP_CMD)

    print("--- read replicated.txt after crash ---")
    cli(5004, "read", "replicated.txt")
    time.sleep(SLEEP_CMD)

    print("--- ls after crash ---")
    cli(5004, "ls")
    time.sleep(SLEEP_CMD)

def demo_cleanup():
    separator("Cleanup")

    print("--- delete music.txt ---")
    cli(5004, "delete", "music.txt")
    time.sleep(SLEEP_CMD)

    print("--- ls after delete ---")
    cli(5004, "ls")
    time.sleep(SLEEP_CMD)

    print("--- read deleted file (should error) ---")
    cli(5004, "read", "music.txt")
    time.sleep(SLEEP_CMD)

    print("--- delete input.csv ---")
    cli(5004, "delete", "input.csv")
    time.sleep(SLEEP_CMD)

    print("--- delete input.csv again (should error) ---")
    cli(5004, "delete", "input.csv")
    time.sleep(SLEEP_CMD)

# ------------------------------------------------------------------ #
#  main                                                                #
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    try:
        start_nodes()
        check_ring()
        demo_dfs()
        demo_replication_paxos()
        demo_sort()
        demo_failure()
        demo_cleanup()
        print("\n=== Demo Complete ===")
    except KeyboardInterrupt:
        print("\nInterrupted")
    finally:
        stop_nodes()
