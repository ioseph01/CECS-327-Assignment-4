
from config import M

class FingerEntry:
  def __init__(self, start: int, node_id=None, address=None):
    self.start = start
    self.id = node_id
    self.address = address

  def __repr__(self):
    return f"FingerEntry(start={self.start}, node_id={self.id}, address={self.address})"


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
      lines.append(f"  [{i}] start={entry.start} -> node_id={entry.id} ({entry.address})")
    return "\n".join(lines)
  
