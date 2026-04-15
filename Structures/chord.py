from config import M, HOST
from Structures.finger_table import FingerTable
from Structures.paxos import Paxos
from Structures.file_objcts import MetaData
from Server.client import send_message
import random
import bisect


class Node:

  def __init__(self, id: int, port: int):
    # self.chord = chord
    self.id = id
    self.host = HOST
    self.port = port
    self.address = f"{self.host}:{self.port}"
    self.pred = None
    self.succ = None
    self.store = {} # Page or MetaData obj
    self.finger_table = FingerTable(id)
    self.sort_buffer = {} # job id : (k,v)
    self.paxos = Paxos(self, self.store)
    self.alive = True
    self.dfs = None

  def parse_addr(self, address):
    host, port = address.split(":")
    return host, int(port)
  
  def send(self, address, msg):
    host, port = self.parse_addr(address)
    return send_message(host, port, msg)
  

  def find_succ(self, key: int):
    if self.succ is None or self.succ == self.address:
      return self.address
    succ_id = self.get_succ_id()
    if self.in_range(key, self.id, succ_id):
      return self.succ
    closest = self.closest_prec_finger(key)
    if closest == self.address:
      return self.succ
    reply = self.send(closest, {
      "type": "find_succ",
      "key": key,
    })
    if reply.get("status") == "error":
      return self.succ
    return reply["address"]
  
  def get_succ_id(self):
    reply = self.send(self.succ, {
      "type": "get_succ"
    })
    if reply.get("status") == "error":
      return self.id
    return reply["id"]
  
  def closest_prec_finger(self, key : int):
    for i in range(M - 1, -1, -1):
      entry = self.finger_table.entries[i]
      if entry.id is not None and self.in_range(entry.id, self.id, key):
        return entry.address
    return self.address
  

  def in_range_open(self, key, lo, hi):
    ''' For stabilizing '''
    if lo == hi:
      return False  # single node, nothing is strictly between
    if lo < hi:
      return lo < key < hi
    return lo < key or key < hi
  

  def in_range(self, key, lo, hi):
    ''' For succsessor '''
    if lo < hi:
      return lo < key <= hi
    elif lo == hi:
      return True
    return lo < key or key <= hi


  def put(self, key: int, value):
    address = self.find_succ(key)
    if address == self.address:
      self.store[key] = value
      return {
        "status": "ok"
      }
    return self.send(address, {
      "type": "put",
      "key": key,
      "value": value if isinstance(value, dict) else value._export()
    })


  def get(self, key: int):
    address = self.find_succ(key)
    if address == self.address:
      return self.store.get(key, None)
    reply = self.send(address, {
      "type": "get",
      "key": key,
    })
    if reply.get("status") == "error" or reply.get("status") is None:
      return None
    return reply["value"]
  

  def delete(self, key: int):
    address = self.find_succ(key)
    if address == self.address:
      if key in self.store:
        del self.store[key]
        return True
      return False
    reply = self.send(address, {
      "type": "delete",
      "key": key,
    })
    return reply.get("deleted", False)
  
  def join(self, addr):
    reply = self.send(addr, {
      "type": "find_succ",
      "key": self.id
    })
    if reply.get("status") == "error":
      print(f"Node failed to join via {addr}")
      return
    if reply["address"] is None:
      return
    self.succ = reply["address"]
    print(f"Node {self.id} joined ring with successor {self.succ}")
      

  def stabilize(self):
    # If pointing to self, try to find real successor via finger table
    if self.succ == self.address:
      addr = self.find_succ((self.id + 1) % (2 ** M))
      if addr != self.address:
        self.succ = addr
      return  # still return, notify will happen next round

    reply = self.send(self.succ, {"type": "get_pred"})
    if reply.get("status") == "error":
      self.succ = self.address
      return
    pred = reply.get("address")
    if pred is not None and pred != self.address:
      pred_id = self.id_from_address(pred)
      succ_id = self.id_from_address(self.succ)
      if self.in_range_open(pred_id, self.id, succ_id):
        self.succ = pred
    self.send(self.succ, {
      "type": "notify",
      "id": self.id,
      "address": self.address,
    })
  def notify(self, sender_id, sender_addr):
      if sender_addr is None or sender_addr == self.address:
          return
      # Update predecessor
      if self.pred is None:
          self.pred = sender_addr
      else:
          pred_id = self.id_from_address(self.pred)
          if self.in_range_open(sender_id, pred_id, self.id):
              self.pred = sender_addr
      # Bootstrap fix: if we still point to ourselves, any notifier is a better successor candidate
      if self.succ == self.address or self.succ is None:
          self.succ = sender_addr

  def fix_fingers(self):
    i = random.randint(0, M - 1)
    start = self.finger_table.entries[i].start
    addr = self.find_succ(start)
    if addr is None:
      return
    id = self.id_from_address(addr)
    self.finger_table.entries[i].id = id
    self.finger_table.entries[i].address = addr

  def id_from_address(self, addr):
    _, port = addr.split(":")
    return int(port) - 5000
    reply = self.send(addr, {
      "type": "get_succ"
    })
    if reply.get("status") == "error":
      return 0
    return reply.get("self_id", 0)


  def handle_find_succ(self, message: dict):
    address = self.find_succ(message["key"])
    return {"status": "ok", "address": address}

  def handle_get_succ(self, message: dict):
    # print(f"  GET_SUCC: Node {self.id} returning succ={self.succ}")
    return {"status": "ok", "id": self.id, "self_id": self.id, "address": self.succ}

  def handle_get_pred(self, message: dict):
    return {"status": "ok", "address": self.pred}

  def handle_notify(self, message: dict):
    self.notify(message["id"], message["address"])
    return {"status": "ok"}

  def handle_put(self, message: dict):
    self.store[message["key"]] = message["value"]
    return {"status": "ok"}

  def handle_get(self, message: dict):
    value = self.store.get(message["key"], None)
    return {"status": "ok", "value": value}

  def handle_delete(self, message: dict):
    key = message["key"]
    if key in self.store:
      del self.store[key]
      return {"status": "ok", "deleted": True}
    return {"status": "ok", "deleted": False}

  def handle_paxos_accept(self, message: dict):
    metadata = MetaData._import(message["metadata"])
    response = self.paxos.f_receive_accept(metadata, message["ballot"])
    return {"status": "ok", "response": response.name}

  def handle_paxos_commit(self, message: dict):
    metadata = MetaData._import(message["metadata"])
    self.paxos.f_receive_commit(metadata, message["ballot"])
    return {"status": "ok"}

  def handle_dfs_touch(self, message: dict):
    return {"status": "ok", "result": self.dfs.touch(message["file_name"])}

  def handle_dfs_append(self, message: dict):
    return {"status": "ok", "result": self.dfs.append(message["file_name"], message["local_path"])}

  def handle_dfs_read(self, message: dict):
    return {"status": "ok", "result": self.dfs.read(message["file_name"])}

  def handle_dfs_head(self, message: dict):
    return {"status": "ok", "result": self.dfs.head(message["file_name"], message["n"])}

  def handle_dfs_tail(self, message: dict):
    return {"status": "ok", "result": self.dfs.tail(message["file_name"], message["n"])}

  def handle_dfs_delete(self, message: dict):
    return {"status": "ok", "result": self.dfs.delete_file(message["file_name"])}

  def handle_dfs_ls(self, message: dict):
    return {"status": "ok", "result": self.dfs.ls()}

  def handle_dfs_stat(self, message: dict):
    # print("Handle dfs stat.")
    return {"status": "ok", "result": self.dfs.stat(message["file_name"])}

  def handle_dfs_sort(self, message: dict):
    return {"status": "ok", "result": self.dfs.sort_file(message["file_name"], message["output"])}

  def handle_sort_route(self, message: dict):
    job_id = message["job_id"]
    k = message["k"]
    v = message["v"]
    if job_id not in self.sort_buffer:
        self.sort_buffer[job_id] = []
    bisect.insort(self.sort_buffer[job_id], (k, v))
    return {"status": "ok"}

  def handle_sort_collect(self, message: dict):
    job_id = message["job_id"]
    records = self.sort_buffer.pop(job_id, [])
    return {"status": "ok", "records": records}
  
  
  def handle_paxos_propose(self, message: dict):
    metadata = MetaData._import(message["metadata"])
    replicas = self.dfs.get_replica_addresses(metadata.key)
    replica_nodes = self.dfs.get_replica_node_objects(replicas)
    success = self.paxos.l_propose(metadata, replica_nodes)
    return {"status": "ok", "committed": success}

  def handle_ls_local(self, message: dict):
    files = []
    for obj in self.store.values():
      if isinstance(obj, dict) and obj.get("type") == "MetaData":
        name = obj.get("file_name")
        if name:
          files.append(name)
      elif hasattr(obj, "type") and obj.type == "MetaData":
        files.append(obj.file_name)
    return {"status": "ok", "files": files}

  def handle_get_ballot(self, message):
    return {
      "status": "ok",
      "ballot": self.paxos.ballot,
    }


  def id_from_str(self, address: str) -> int:
    _, port = self.parse_address(address)
    return port - 5000

  def print_info(self):
    print(f"Node {self.id} | address={self.address} | succ={self.succ} | pred={self.pred}")
    print(self.finger_table)
    

  def __repr__(self):
    return f"Node(id={self.id}, pred={self.pred}, succ={self.succ}, store_keys={list(self.store.keys())})"
