from utils.hash import hash_key

class Page:

  def __init__(self, key : str, contents : str, page_number : int):
    self.key = key
    self.contents = contents
    self.page_number = page_number

  @property
  def size(self):
    return len(self.contents)
  
  @property
  def type(self):
    return "Page"
  

  @classmethod
  def _export(self) -> dict:
    return {
        "key": self.key,
        "contents": self.contents,
        "page_number": self.page_number,
        "size": self.size,
        "type": self.type,

    }
  
  @classmethod
  def _import(cls, data: dict) -> "Page":
    return cls(
        key=data["key"],
        contents=data["contents"],
        page_number=data["page_number"],
    )


class MetaData:

  def __init__(self, key : str, file_name : str, page_keys : list, file_size=0, replica_nodes=[]):
    self.key = key
    self.file_name = file_name
    self.page_keys = page_keys
    self.file_size = file_size
    self.replica_nodes = replica_nodes

  @property
  def page_count(self):
    return len(self.page_keys)
  
  @property
  def type(self):
    return "MetaData"
  

  def _export(self) -> dict:
    return {
        "key": self.key,
        "file_name": self.file_name,
        "file_size": self.file_size,
        "page_keys": self.page_keys,
        "page_count": self.page_count,
        "type": self.type,
        "replica_nodes": self.replica_nodes
    }
  
  @classmethod
  def _import(cls, data: dict) -> "MetaData":
    return cls(
        key=data["key"],
        file_name=data["file_name"],
        page_keys=data.get("page_keys", []),
        file_size=data["file_size"],
        replica_nodes=data.get("replica_nodes", []),
    )
  



