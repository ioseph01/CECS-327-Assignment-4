from config import R

""" 
  Replica of R will be:
  Primary Node, Successor Node, Successor Node
"""

def get_replica_nodes(chord, primary_node):
  ''' Return R nodes starting with the primary node '''
  sorted_ids = chord.sorted_ids
  n = len(sorted_ids)

  if n < R:
    return [chord.node[id] for id in sorted_ids]
  
  start = sorted_ids.index(primary_node.id)
  replicas = []
  for i in range(R):
    id = sorted_ids[(start + i) % n]
    replicas.append(chord.nodes[id])

  return replicas


def read_replicas(key: int, replica_nodes: list):
  ''' Returns the value of a replica '''
  for node in replica_nodes:
    value = node.store.get(key, None)
    if value is not None:
      return value
  return None


def write_replicas(key: int, value, replica_nodes: list):
  ''' Write value to all replicas '''
  for node in replica_nodes:
    node.store[key] = value