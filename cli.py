# cli.py
import argparse
from Server.client import send_message
from config import HOST

def parse_args():
    parser = argparse.ArgumentParser(description="DFS Client CLI")
    parser.add_argument("--host", type=str, default=HOST, help="Node host")
    parser.add_argument("--port", type=int, required=True, help="Node port")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # touch
    p = subparsers.add_parser("touch")
    p.add_argument("file_name")

    # append
    p = subparsers.add_parser("append")
    p.add_argument("file_name")