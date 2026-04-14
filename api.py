from utils.hash import hash_key
from Structures.file_objcts import Page, MetaData
from Structures.replica import *
from config import M, PAGE_SIZE
import bisect
import uuid

class DFS:
  def __init__(self, chord, entry_node):
    self.chord = chord
    self.entry_node = entry_node

  def metadata_key(self, file_name : str):
    return hash_key("metadata:" + file_name)

  def page_key(self, file_name : str, page_number):
    return hash_key(file_name + ":" + str(page_number))
  
  def get_metadata(self, file_name):
    key = self.metadata_key(file_name)
    replicas = self.get_replicas(key)
    obj = read_replicas(key, replicas)


    if obj is None:
      return None
    elif isinstance(obj, dict):
      return MetaData._import(obj)
    return obj
  
  def put_metadata(self, metadata):
    replicas = self.get_replicas(metadata.key)
    metadata.replica_nodes = [node.id for node in replicas]

    leader = min(replicas, key=lambda node : node.id)
    leader.paxos.l_propose(metadata, replicas)
    # write_replicas(metadata.key, metadata, replicas)
    # self.entry_node.put(metadata.key, metadata)

  def get_page(self, page_key : int):
    obj = self.entry_node.get(page_key)

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
        self.entry_node.put(page_key, page)
        metadata.page_keys.append(page_key)
        metadata.file_size += len(chunk)

    self.put_metadata(metadata)
    return f"SUCCESS: appended {len(chunks)} page(s) to '{file_name}'"
  
  def get_replicas(self, key):
    primary = self.chord.find_succ(key)
    return get_replica_nodes(self.chord, primary)


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
      metadata = self.get_metadata(file_name)

    chunks = [contents[i:i + PAGE_SIZE] for i in range(0, len(contents), PAGE_SIZE)]
    if not chunks or chunks is None:
      return f"FAILURE: File '{local_path}' is empty, nothing appended"
    
    for chunk in chunks:
      page_number = metadata.page_count
      page_key = self.page_key(file_name, page_number)
      page = Page(page_key, chunk, page_number)

      self.entry_node.put(page_key, page)
      metadata.page_keys.append(page_key)
      metadata.file_size += 1

    self.put_metadata(metadata)
    return f"SUCCESS: Appended {len(chunks)} to '{file_name}'"
  

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
    
    lines = contents.splitlines()
    return "\n".join(lines[:n])
  

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
      self.entry_node.delete(page_key)

    replicas = self.get_replicas(metadata.key)
    for node in replicas:
      node.store.pop(metadata.key, None)
    return f"SUCCESS: '{file_name}' deleted"
  

  def ls(self):
    ''' List all files in chord '''
    files = []
    seen = set()
    for node in self.chord.nodes.values():
      for _, obj in node.store.items():
        if obj.type == 'MetaData' and obj.file_name not in seen:
          files.append(obj.file_name)
          seen.add(obj.file_name)

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
    
    # Get pages and parse
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
          k,v = line.split(",", 1)
          records.append((k.strip(), v.strip()))

    if not records:
      return f"ERROR: no valid records found in '{file_name}'"
    
    # Sort
    job_id = str(uuid.uuid4())
    for k,v in records:
      target_key = hash_key(k)
      target_node = self.chord.find_succ(target_key)
      if job_id not in target_node.sort_buffer:
        target_node.sort_buffer[job_id] = []
      bisect.insort(target_node.sort_buffer[job_id], (k,v))

    # Store
    sorted_records = []
    for id in self.chord.sorted_ids:
      node = self.chord.nodes[id]
      if job_id in node.sort_buffer:
        sorted_records.extend(node.sort_buffer[job_id])
    sorted_records.sort(key=lambda pair: pair[0])

    # Output
    contents = "\n".join(f"{k},{v}" for k,v in sorted_records)
    self.touch(output_filename)
    meta_check = self.get_metadata("output.csv")
    print(f"metadata immediately after touch: {meta_check._export() if meta_check else None}")
    self.append_contents(output_filename, contents)
    meta_check2 = self.get_metadata("output.csv")
    print(f"metadata after append_contents: {meta_check2._export() if meta_check2 else None}")

    for node in self.chord.nodes.values():
      if job_id in node.sort_buffer:
        del node.sort_buffer[job_id]

    return f"SUCCSS: Sorted {len(sorted_records)} records from '{file_name}' into '{output_filename}'"
        