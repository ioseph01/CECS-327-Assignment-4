from utils.hash import hash_key
from Structures.file_objcts import Page, MetaData
from Structures.replica import *
from Structures.paxos import ProxyNode
from config import PAGE_SIZE, R
import bisect
import uuid
import time

class DFS:
  def __init__(self, node):
    self.node = node


  def get_replica_addresses(self, key: int):
    primary_address = self.node.find_succ(key)
    return self.get_successive_addresses(primary_address, R) 

  def get_successive_addresses(self, start_address: str, count: int):
    addresses = []
    current = start_address
    visited = set()

    while len(addresses) < count:
      if current in visited:
        break
      visited.add(current)
      addresses.append(current)

      if current == self.node.address:
        next_address = self.node.succ
      else:
        reply = self.node.send(current, {"type": "get_succ"})
        if reply.get("status") == "error":
          break
        next_address = reply.get("address")

      if next_address is None:
        break
      current = next_address

    return addresses

  def id_from_address(self, address: str):
      _, port = address.split(":")
      return int(port) - 5000

  def get_replica_node_objects(self, addresses: list):
      return [ProxyNode(addr, self.node) for addr in addresses]

  def metadata_key(self, file_name : str):
    return hash_key("metadata:" + file_name)

  def page_key(self, file_name : str, page_number):
    return hash_key(file_name + ":" + str(page_number))
  
  def get_metadata(self, file_name):
    key = self.metadata_key(file_name)
    replicas = self.get_replica_addresses(key)

    for addr in replicas:
      if addr == self.node.address:
        obj = self.node.store.get(key, None)
      else:
        reply = self.node.send(addr, {
          "type": "get",
          "key": key
        })
        obj = reply.get("value", None)

      if obj is None:
        continue
      elif isinstance(obj, dict):
        return MetaData._import(obj)
      return obj
    
    return None
  

  def put_metadata(self, metadata):
    replicas = self.get_replica_addresses(metadata.key)
    # metadata.replica_nodes = replicas

    lead_addr = min(replicas, key=lambda addr : self.id_from_address(addr))
    if lead_addr == self.node.address:
      metadata.replica_nodes = self.get_replica_node_objects(replicas)
      self.node.paxos.l_propose(metadata, metadata.replica_nodes)
    else:
      self.node.send(lead_addr, {
        "type": "paxos_propose",
        "metadata": metadata._export(),
      })

  def get_page(self, page_key : int):
    obj = self.node.get(page_key)

    if obj is None:
      return None
    elif isinstance(obj, dict):
      return Page._import(obj)
    return obj
  

  def append_contents(self, file_name: str, contents: str):
    ''' Append string to file '''
    metadata = self.get_metadata(file_name)
    if metadata is None:
      return f"ERROR: File '{file_name}' not found"
    chunks = [contents[i:i + PAGE_SIZE] for i in range(0, len(contents), PAGE_SIZE)]
    if not chunks:
      return f"ERROR: Nothing to append"
    
    for chunk in chunks:
        page_number = metadata.page_count
        page_key = self.page_key(file_name, page_number)
        page = Page(page_key, chunk, page_number)
        self.node.put(page_key, page)
        metadata.page_keys.append(page_key)
        metadata.file_size += len(chunk)

    self.put_metadata(metadata)
    return f"SUCCESS: appended {len(chunks)} page(s) to '{file_name}'"


  """ Required DFS Operations """

  def touch(self, file_name : str):
    ''' Create empty file '''
    existing = self.get_metadata(file_name)
    if existing is not None:
      return f"ERROR: File '{file_name}' already exists"
    
    key = self.metadata_key(file_name)
    metadata = MetaData(key, file_name, [], 0)
    self.put_metadata(metadata)
    return f"SUCCESS: '{file_name}' created"
  

  def append(self, file_name : str, local_path : str):
    ''' Read a local file and append contents '''
    try:
      with open(local_path, "r") as f:
        contents = f.read()
    except FileNotFoundError:
      return f"ERROR: File '{local_path}' not found"
  
    metadata = self.get_metadata(file_name)
    if metadata is None:
      self.touch(file_name)
    return self.append_contents(file_name, contents)
  

  def read(self, file_name: str):
    ''' Read and return file contents '''
    metadata = self.get_metadata(file_name)
    if metadata is None:
      return f"ERROR: File '{file_name}' not found"
    elif metadata.page_count == 0:
      return ""
    
    contents = []
    for key in metadata.page_keys:
      page = self.get_page(key)
      if page is None:
        return f"ERROR: page {key} missing for '{file_name}'"
      contents.append(page.contents)

    return "".join(contents)
  

  def head(self, file_name: str, n: int):
    ''' Return first n lines of a file '''
    contents = self.read(file_name)
    if contents[:5] == "ERROR":
      return contents
    return "\n".join(contents.splitlines()[:n])

  def tail(self, file_name: str, n: int):
    ''' Return last n lines of a file '''
    contents = self.read(file_name)
    if contents[:5] == "ERROR":
      return contents
    
    lines = contents.splitlines()
    return "\n".join(lines[-n:])
  

  def delete_file(self, file_name: str):
    ''' Delete all pages and metadata of a file '''
    metadata = self.get_metadata(file_name)

    if metadata is None:
      return f"ERROR: File '{file_name}' not found"
    
    for page_key in metadata.page_keys:
      self.node.delete(page_key)

    for addr in metadata.replica_nodes:
      if addr == self.node.address:
        self.node.store.pop(metadata.key, None)
      else:
        self.node.send(addr, {
          "type": "delete",
          "key": metadata.key,
        })
    return f"SUCCESS: '{file_name}' deleted"
  

  def ls(self):
    ''' List all files across the entire Chord ring '''
    files = set()

    def collect_from_store(store):
      for obj in store.values():
        if isinstance(obj, dict) and obj.get("type") == "MetaData":
          name = obj.get("file_name")
          if name:
            files.add(name)
        elif hasattr(obj, "type") and obj.type == "MetaData":
          files.add(obj.file_name)

    # collect from local node
    collect_from_store(self.node.store)

    # walk ring and collect from each remote node
    visited = set()
    visited.add(self.node.address)
    current = self.node.succ
    while current and current not in visited:
      visited.add(current)
      reply = self.node.send(current, {"type": "ls_local"})
      if reply.get("status") != "error":
        for name in reply.get("files", []):
          files.add(name)
      # advance to next successor
      reply2 = self.node.send(current, {"type": "get_succ"})
      if reply2.get("status") == "error":
        break
      nxt = reply2.get("address")
      if nxt is None or nxt == self.node.address:
        break
      current = nxt

    if not files:
      return "Directory empty"
    return "\n".join(sorted(files))
    

  def stat(self, file_name : str):
    ''' Return metadata for a file '''
    metadata = self.get_metadata(file_name)
    if metadata is None:
      return f"ERROR: File '{file_name}' does not exist"
    info = metadata._export()
    lines = [f"{k}: {v}" for k, v in info.items()]
    return "\n".join(lines)
  

  def sort_file(self, file_name : str, output_filename : str):
    metadata = self.get_metadata(file_name)
    if metadata is None:
      return f"ERROR: File '{file_name}' does not exist"
    elif metadata.page_count == 0:
        return f"ERROR: File '{file_name}' is empty"
    
    # Parse all records from pages
    records = []
    for page_key in metadata.page_keys:
      page = self.get_page(page_key)
      if page is None:
        return f"ERROR: Page missing in file '{file_name}'"
      for line in page.contents.splitlines():
        line = line.strip()
        if line:
          if "," not in line:
            return f"ERROR: invalid record format '{line}', expected key,value"
          k, v = line.split(",", 1)
          records.append((k.strip(), v.strip()))

    if not records:
      return f"ERROR: no valid records found in '{file_name}'"
    
    # Route each record to the node responsible for hash(key)
    job_id = str(uuid.uuid4())
    remote_targets = set()
    for k, v in records:
      target_addr = self.node.find_succ(hash_key(k))
      if target_addr == self.node.address:
        if job_id not in self.node.sort_buffer:
          self.node.sort_buffer[job_id] = []
        bisect.insort(self.node.sort_buffer[job_id], (k, v))
      else:
        remote_targets.add(target_addr)
        self.node.send(target_addr, {
          "type": "sort_route",
          "job_id": job_id,
          "k": k,
          "v": v,
        })

    if remote_targets:
      time.sleep(0.5)

    # Collect sorted records from every node in ring
    sorted_records = []
    visited = set()
    current = self.node.address
    while True:
      if current in visited:
        break
      visited.add(current)
      if current == self.node.address:
        records_here = self.node.sort_buffer.pop(job_id, [])
      else:
        reply = self.node.send(current, {
          "type": "sort_collect",
          "job_id": job_id,
        })
        records_here = reply.get("records", [])
      sorted_records.extend(records_here)
      reply = self.node.send(current, {"type": "get_succ"})
      if reply.get("status") == "error":
        break
      next_address = reply.get("address")
      if next_address is None:
        break
      current = next_address

    sorted_records.sort(key=lambda pair: pair[0])

    # Write sorted output as new DFS file
    contents = "\n".join(f"{k},{v}" for k, v in sorted_records)
    self.touch(output_filename)
    self.append_contents(output_filename, contents)

    return f"SUCCESS: Sorted {len(sorted_records)} records from '{file_name}' into '{output_filename}'" 