# main.py
import argparse
import threading
import time
from Structures.chord import Node
from api import DFS
from Server.server import Server
from config import STABILIZE_INTERVAL

def parse_args():
    parser = argparse.ArgumentParser(description="Start a DFS Chord node")
    parser.add_argument("--id",        type=int, required=True,  help="Node ID")
    parser.add_argument("--port",      type=int, required=True,  help="Port to listen on")
    parser.add_argument("--bootstrap", type=str, default=None,   help="Bootstrap node address e.g. localhost:5004")
    return parser.parse_args()

def stabilize_loop(node, interval: float):
    while node.alive:
        try:
            node.stabilize()
        except Exception as e:
            print(f"[Stabilize] Error: {e}")
        time.sleep(interval)

def fix_fingers_loop(node, interval: float):
    while node.alive:
        try:
            node.fix_fingers()
        except Exception as e:
            print(f"[FixFingers] Error: {e}")
        time.sleep(interval)

def main():
    args = parse_args()

    # create node
    node = Node(args.id, args.port)

    # create DFS and wire it to the node
    dfs = DFS(node)
    node.dfs = dfs

    # start TCP server in background
    server = Server(node, args.port)
    server.start()

    # give server a moment to bind
    time.sleep(0.5)

    # join ring if bootstrap address provided
    if args.bootstrap:
        node.join(args.bootstrap)
    else:
        print(f"[Node {args.id}] Starting as bootstrap node")

    # start stabilize loop in background thread
    stab_thread = threading.Thread(
        target=stabilize_loop,
        args=(node, STABILIZE_INTERVAL),
        daemon=True
    )
    stab_thread.start()

    # start fix_fingers loop in background thread
    fix_thread = threading.Thread(
        target=fix_fingers_loop,
        args=(node, STABILIZE_INTERVAL),
        daemon=True
    )
    fix_thread.start()

    print(f"[Node {args.id}] Ready on port {args.port}")

    # block forever
    try:
        while node.alive:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[Node {args.id}] Shutting down")
        node.alive = False
        server.stop()

if __name__ == "__main__":
    main()