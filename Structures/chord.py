from config import M
from Structures.finger_table import FingerTable


class Chord:
  def __init__(self):
    self.nodes = {} # id : node


  @property
  def sorted_ids(self):
    return sorted(self.nodes.keys())
  

  def find_succ(self, key : int):
    sorted_ids = self.sorted_ids
    if not sorted_ids:
      return None
    
    for id in sorted_ids:
      if key <= id:
        return self.nodes[id]
      
    return self.nodes[sorted_ids[0]]
  

  def update_finger_table(self, node):
    node.finger_table = FingerTable(node.id)
    for entry in node.finger_table.entries:
      entry.node = self.find_succ(entry.start)
  

  def update_all_finger_tables(self):
    for node in self.nodes.values():
      self.update_finger_table(node)


  def update_all_pred(self):
    sorted_ids = self.sorted_ids
    for i, id in enumerate(sorted_ids):
      pred_id = sorted_ids[(i - 1) % len(sorted_ids)]
      self.nodes[id].pred = self.nodes[pred_id]

      succ_id = sorted_ids[(i + 1) % len(sorted_ids)]
      self.nodes[id].succ = self.nodes[succ_id]


  def update(self):
    self.update_all_finger_tables()
    self.update_all_pred()


  def add_node(self, node):
    self.nodes[node.id] = node
    self.update()


  def remove_node(self, id : int):
    if id in self.nodes:
      del self.nodes[id]
      self.update()


  def print_ring(self):
    print("=== Chord Ring ===")
    for node_id in self.sorted_ids:
        node = self.nodes[node_id]
        pred_id = node.pred.id if node.pred else None
        succ_id = node.succ.id if node.succ else None
        print(f"  Node {node_id:3d} | pred={pred_id} | succ={succ_id}")
    print("==================")


  def print_finger_tables(self):
    print("=== Finger Tables ===")
    for node_id in self.sorted_ids:
        print(self.nodes[node_id].finger_table)
        print()
    print("=====================")



class Node:

  def __init__(self, chord : Chord, id):
    self.chord = chord
    self.id = id
    self.pred = None
    self.succ = None
    self.store = {} # Page or MetaData obj
    self.finger_table = FingerTable(id)
    self.sort_buffer = {} # job id : (k,v)


  def set(self, key: int, value):
    target = self.chord.find_succ(key)
    target.store[key] = value


  def get(self, key: int):
    target = self.chord.find_succ(key)
    return target.store.get(key, None)
  

  def delete(self, key: int):
    target = self.chord.find_succ(key)
    if key in target.store:
      del target.store[key]
      return True
    return False
  

  def __repr__(self):
    pred_id = self.pred.id if self.pred else None
    succ_id = self.succ.id if self.succ else None
    return f"Node(id={self.id}, pred={pred_id}, succ={succ_id}, store_keys={list(self.store.keys())})"
