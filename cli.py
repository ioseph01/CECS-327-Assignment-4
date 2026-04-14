# cli.py
import argparse
from Server.client import send_message
from config import HOST


def parse_args():
    parser = argparse.ArgumentParser(description="DFS Client CLI")
    parser.add_argument("--host", type=str, default=HOST, help="Node host")
    parser.add_argument("--port", type=int, required=True, help="Node port")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # touch <file_name>
    p = subparsers.add_parser("touch")
    p.add_argument("file_name")

    # append <file_name> <local_path>
    p = subparsers.add_parser("append")
    p.add_argument("file_name")
    p.add_argument("local_path")

    # read <file_name>
    p = subparsers.add_parser("read")
    p.add_argument("file_name")

    # head <file_name> <n>
    p = subparsers.add_parser("head")
    p.add_argument("file_name")
    p.add_argument("n", type=int)

    # tail <file_name> <n>
    p = subparsers.add_parser("tail")
    p.add_argument("file_name")
    p.add_argument("n", type=int)

    # delete <file_name>
    p = subparsers.add_parser("delete")
    p.add_argument("file_name")

    # ls
    subparsers.add_parser("ls")

    # stat <file_name>
    p = subparsers.add_parser("stat")
    p.add_argument("file_name")

    # sort <file_name> <output_filename>
    p = subparsers.add_parser("sort")
    p.add_argument("file_name")
    p.add_argument("output")

    return parser.parse_args()


def main():
    args = parse_args()
    host = args.host
    port = args.port

    if args.command == "touch":
        reply = send_message(host, port, {
            "type": "dfs_touch",
            "file_name": args.file_name,
        })
        print(reply.get("result", reply))

    elif args.command == "append":
        reply = send_message(host, port, {
            "type": "dfs_append",
            "file_name": args.file_name,
            "local_path": args.local_path,
        })
        print(reply.get("result", reply))

    elif args.command == "read":
        reply = send_message(host, port, {
            "type": "dfs_read",
            "file_name": args.file_name,
        })
        print(reply.get("result", reply))

    elif args.command == "head":
        reply = send_message(host, port, {
            "type": "dfs_head",
            "file_name": args.file_name,
            "n": args.n,
        })
        print(reply.get("result", reply))

    elif args.command == "tail":
        reply = send_message(host, port, {
            "type": "dfs_tail",
            "file_name": args.file_name,
            "n": args.n,
        })
        print(reply.get("result", reply))

    elif args.command == "delete":
        reply = send_message(host, port, {
            "type": "dfs_delete",
            "file_name": args.file_name,
        })
        print(reply.get("result", reply))

    elif args.command == "ls":
        reply = send_message(host, port, {
            "type": "dfs_ls",
        })
        print(reply.get("result", reply))

    elif args.command == "stat":
        reply = send_message(host, port, {
            "type": "dfs_stat",
            "file_name": args.file_name,
        })
        print(reply.get("result", reply))

    elif args.command == "sort":
        reply = send_message(host, port, {
            "type": "dfs_sort",
            "file_name": args.file_name,
            "output": args.output,
        })
        print(reply.get("result", reply))


if __name__ == "__main__":
    main()