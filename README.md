# CECS-327-Assignment-4
Distributed File system

# Libraries

 - subprocess 
 - argparse
 - socket
 - threading
 - bisect
 - hashlib
 - uuid
 - time

# Running
Run with `python3 demo.py`.

# Manual Testing
In one terminal, launch a node with `python3 main.py --id <node_id> --port <port_num>` where node_id is an integer and port number is 5000 + node_id.

Without closing that terminal, navigate to another terminal and **issue commands** to it with `python3 cli.py`.

**Example:** `python3 main.py --id 4 --port 5004`
             `python3 cli.py --port 5004 touch file.txt` 

However, to actually connect new nodes to a ring, set one node as the network node like localhost:5004.

**Example:** `python3 main.py --id 8 --port 8 --bootstrap localhost:5004`

And list the commands you can run through the command-line interface with `python3 cli.py -h`.

Using the above, run the <u>**sort command**</u> assuming a file has the proper records format with `python3 cli.py --port <port_num> sort <in_file> <out_file>`.

**Example:** `python3 cli.py --port 5008 sort t.txt out.txt`

# Paxos
Paxos should automatically run whenever metadata is created or updated because our replication strategy with R=3 nodes only includes replicating metadata. The output of paxos voting should be apparent in outputs with multiple nodes.
