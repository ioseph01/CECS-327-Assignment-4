# dfs_test.py
import time
import subprocess
import sys
import os
import signal
from Server.client import send_message

HOST = "localhost"
PORTS = [5004, 5008, 5015, 5027, 5044]
NODE_IDS = [4, 8, 15, 27, 44]
BOOTSTRAP_PORT = 5004
PROCESSES = []

# ------------------------------------------------------------------ #
#  node management                                                     #
# ------------------------------------------------------------------ #

def start_nodes():
    print("=== Starting Chord Ring ===")

    # start bootstrap node
    p = subprocess.Popen(
        [sys.executable, "main.py", "--id", "4", "--port", "5004"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    PROCESSES.append(p)
    print(f"Started bootstrap node 4 on port 5004 (pid={p.pid})")
    time.sleep(2)

    # start remaining nodes
    for node_id, port in zip(NODE_IDS[1:], PORTS[1:]):
        p = subprocess.Popen(
            [sys.executable, "main.py",
             "--id", str(node_id),
             "--port", str(port),
             "--bootstrap", f"localhost:{BOOTSTRAP_PORT}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        PROCESSES.append(p)
        print(f"Started node {node_id} on port {port} (pid={p.pid})")
        time.sleep(2)

    print("\nWaiting 10s for ring to stabilize...")
    time.sleep(10)
    print("Ring ready.\n")

def stop_nodes():
    print("\n=== Shutting down nodes ===")
    for p in PROCESSES:
        try:
            p.terminate()
            p.wait(timeout=3)
        except Exception:
            p.kill()
    print("Done.")

# ------------------------------------------------------------------ #
#  helpers                                                             #
# ------------------------------------------------------------------ #

def send(port, message):
    return send_message(HOST, port, message)

def dfs(port, command, **kwargs):
    msg = {"type": f"dfs_{command}", **kwargs}
    reply = send(port, msg)
    return reply.get("result", reply)

def separator(title):
    print(f"\n{'='*10} {title} {'='*10}")

def check(label, result, expected=None):
    if expected is not None:
        passed = expected in str(result)
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {label}")
        if not passed:
            print(f"         expected: {expected}")
            print(f"         got:      {result}")
    else:
        print(f"  [INFO] {label}: {result}")

# ------------------------------------------------------------------ #
#  tests                                                               #
# ------------------------------------------------------------------ #

def test_chord():
    separator("Chord Connectivity")
    for node_id, port in zip(NODE_IDS, PORTS):
        reply = send(port, {"type": "get_succ"})
        check(f"node {node_id} on port {port} reachable", reply, "ok")
        print(f"         succ={reply.get('address')} self_id={reply.get('self_id')}")

def test_touch():
    separator("touch")
    result = dfs(5004, "touch", file_name="test.txt")
    check("touch new file", result, "SUCCESS")

    result = dfs(5004, "touch", file_name="test.txt")
    check("touch duplicate rejected", result, "ERROR")

def test_stat():
    separator("stat")
    result = dfs(5004, "stat", file_name="test.txt")
    check("stat existing file", result, "test.txt")
    check("stat shows replica_nodes", result, "replica_nodes")

    result = dfs(5004, "stat", file_name="nonexistent.txt")
    check("stat missing file returns error", result, "ERROR")

def test_append_and_read():
    separator("append and read")

    with open("tmp/a1.txt", "w") as f:
        f.write("line one\nline two\nline three\n")
    with open("tmp/a2.txt", "w") as f:
        f.write("line four\nline five\nline six\n")
    with open("tmp/a3.txt", "w") as f:
        f.write("line seven\nline eight\nline nine\n")

    result = dfs(5004, "append", file_name="test.txt", local_path="tmp/a1.txt")
    check("append page 1", result, "SUCCESS")

    result = dfs(5008, "append", file_name="test.txt", local_path="tmp/a2.txt")
    check("append page 2 via different node", result, "SUCCESS")

    result = dfs(5015, "append", file_name="test.txt", local_path="tmp/a3.txt")
    check("append page 3 via different node", result, "SUCCESS")

    result = dfs(5004, "read", file_name="test.txt")
    check("read returns all content", result, "line one")
    check("read contains page 2", result, "line four")
    check("read contains page 3", result, "line seven")

def test_head_tail():
    separator("head and tail")
    result = dfs(5004, "head", file_name="test.txt", n=2)
    check("head 2 lines", result, "line one")

    result = dfs(5004, "tail", file_name="test.txt", n=2)
    check("tail 2 lines", result, "line nine")

def test_ls():
    separator("ls")
    dfs(5004, "touch", file_name="file_a.txt")
    dfs(5004, "touch", file_name="file_b.txt")

    result = dfs(5004, "ls")
    check("ls shows test.txt", result, "test.txt")

def test_delete():
    separator("delete")
    result = dfs(5004, "delete", file_name="test.txt")
    check("delete existing file", result, "SUCCESS")

    result = dfs(5004, "read", file_name="test.txt")
    check("read after delete returns error", result, "ERROR")

    result = dfs(5004, "delete", file_name="nonexistent.txt")
    check("delete missing file returns error", result, "ERROR")

def test_sort():
    separator("distributed sort")

    import random
    with open("tmp/sort_in.txt", "w") as f:
        keys = [f"{random.randint(0, 9999):04d}" for _ in range(50)]
        for i, k in enumerate(keys):
            f.write(f"{k},value{i}\n")

    dfs(5004, "touch", file_name="sort_input.txt")
    result = dfs(5004, "append", file_name="sort_input.txt", local_path="tmp/sort_in.txt")
    check("append sort input", result, "SUCCESS")

    result = dfs(5004, "sort", file_name="sort_input.txt", output="sort_output.txt")
    print(f"RESULT: {result}")
    check("sort_file completes", result, "SUCCESS")

    result = dfs(5004, "read", file_name="sort_output.txt")
    check("sorted output exists", result, ",value")

    lines = [l for l in result.strip().split("\n") if l]
    keys_out = [l.split(",")[0] for l in lines]
    is_sorted = keys_out == sorted(keys_out)
    print(f"  [{'PASS' if is_sorted else 'FAIL'}] keys in sorted order: {is_sorted}")

def test_replication():
    separator("replication")
    dfs(5004, "touch", file_name="rep.txt")

    with open("tmp/rep.txt", "w") as f:
        f.write("replicated content\n" * 5)

    dfs(5004, "append", file_name="rep.txt", local_path="tmp/rep.txt")
    result = dfs(5004, "stat", file_name="rep.txt")
    check("stat shows replicas", result, "replica_nodes")
    print(f"  [INFO] stat:\n{result}")

def test_paxos():
    separator("Paxos")
    dfs(5004, "touch", file_name="paxos.txt")

    with open("tmp/paxos.txt", "w") as f:
        f.write("paxos test content\n" * 5)

    result = dfs(5004, "append", file_name="paxos.txt", local_path="tmp/paxos.txt")
    check("append triggers Paxos commit", result, "SUCCESS")

    result = dfs(5004, "read", file_name="paxos.txt")
    check("read after Paxos commit", result, "paxos test content")

def test_failure():
    separator("failure scenario")

    dfs(5004, "touch", file_name="failure.txt")
    with open("tmp/failure.txt", "w") as f:
        f.write("before crash\n" * 5)
    dfs(5004, "append", file_name="failure.txt", local_path="tmp/failure.txt")

    # crash node 44
    crashed = PROCESSES[-1]
    print(f"  Crashing node 44 (pid={crashed.pid})")
    crashed.terminate()
    crashed.wait(timeout=3)
    time.sleep(1)

    # append should still work with remaining nodes
    with open("tmp/after_crash.txt", "w") as f:
        f.write("after crash\n" * 5)
    result = dfs(5004, "append", file_name="failure.txt", local_path="tmp/after_crash.txt")
    check("append after crash succeeds", result, "SUCCESS")
    result = dfs(5004, "ls")
    check("result for ls", result)
    result = dfs(5004, "read", file_name="failure.txt")
    check("read after crash returns content", result, "before crash")
    check("read contains post-crash content", result, "after crash")

# ------------------------------------------------------------------ #
#  main                                                                #
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    try:
        start_nodes()
        test_chord()
        test_touch()
        test_stat()
        test_append_and_read()
        test_head_tail()
        test_ls()
        test_delete()
        test_sort()
        test_replication()
        test_paxos()
        test_failure()
        print("\n=== All Tests Complete ===")
    except KeyboardInterrupt:
        print("\nInterrupted")
    finally:
        stop_nodes()