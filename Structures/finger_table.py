
from config import M

class FingerEntry:
  def __init__(self, start: int, node=None):
    self.start = start
    self.node = node

  def __repr__(self):
    node_id = self.node.id if self.node else None
    return f"FingerEntry(start={self.start}, node={node_id})"


class FingerTable:
  def __init__(self, owner_id: int):
    self.owner_id = owner_id
    self.entries = [
        FingerEntry(start=(owner_id + 2**i) % (2**M))
        for i in range(M)
    ]

  def __repr__(self):
    lines = [f"FingerTable for node {self.owner_id}:"]
    for i, entry in enumerate(self.entries):
        lines.append(f"  [{i}] start={entry.start} -> node={entry.node.id if entry.node else None}")
    return "\n".join(lines)
  
