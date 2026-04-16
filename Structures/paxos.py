from enum import Enum


class Status(Enum):
  LEARN = 0
  REJECT = 1


class Paxos:

  def __init__(self, node, store):
    self.node = node
    self.ballot = 0
    self.store = store
    self.log = []
    self.pending = {}

  @property
  def id(self):
    return self.node.id

  def l_propose(self, metadata, replica_nodes : list):
    ''' Leader proposes a metadata update '''
    print(f"  PAXOS: Starting propose, ballot will be {self.ballot + 1}, replica ids={[r.id for r in replica_nodes]}")
    alive_replicas = [r for r in replica_nodes if r.alive]
    if not alive_replicas:
      print(f"  PAXOS: No alive replicas, aborting")
      return False
    
    leader_id = min(replica.id for replica in alive_replicas)
    if self.id != leader_id or not self.node.alive:
      return False
    
    self.ballot = 1 + max(replica.paxos.ballot for replica in alive_replicas)
    ballot = self.ballot

    print(f"\n  PAXOS: Leader: node {self.id}")
    print(f"  PAXOS: Proposing ballot {ballot} for '{metadata.file_name}'")

    c = 0
    for replica in alive_replicas:
      print(f"  PAXOS: Sending ACCEPT ({ballot}) to node {replica.id}")
      response = replica.paxos.f_receive_accept(metadata, ballot)
      if response == Status.LEARN:
        print(f"  PAXOS: Received LEARN ({ballot}) from node {replica.id}")
        c += 1
      else:
        print(f"  PAXOS: Response did not receive a LEARN, {response}")

    majority = len(alive_replicas) // 2 + 1
    if majority <= c:
      print(f"  PAXOS: Majority reached ({c}/{len(replica_nodes)}), committing")
      for replica in alive_replicas:
        replica.paxos.f_receive_commit(metadata, ballot)
      return True
    
    print(f"  PAXOS: Majority not reached ({c}/{len(replica_nodes)}), aborting")
    return False


  def f_receive_accept(self, metadata, ballot : int):
    if self.ballot <= ballot and self.node.alive:
      self.ballot = ballot
      self.pending[ballot] = metadata
      print(f"  PAXOS: Node {self.id} accepting ballot {ballot}, self.ballot={self.ballot}")
      return Status.LEARN
    print(f"  PAXOS: Node {self.id} rejecting ballot {ballot}, self.ballot={self.ballot}")
    return Status.REJECT
  

  def f_receive_commit(self, metadata, ballot : int):
    self.store[metadata.key] = metadata._export()
    self.log.append({
      "ballot": ballot,
      "file_name": metadata.file_name,
      "metadata": metadata._export()
    })
    if ballot in self.pending:
      del self.pending[ballot]
    print(f"  PAXOS: Node {self.id} committing ballot {ballot} for {metadata.file_name}.")


  def print_log(self) -> None:
    print(f"\n  PAXOS: Log for node {self.id}:")
    if not self.log:
        print("    Empty Log")
    for log in self.log:
        print(f"    ballot={log['ballot']}, file={log['file_name']}")


class ProxyNode:
  def __init__(self, address, local_node):
    self.address = address
    self.node = local_node
    _, port = address.split(":")
    self.id = int(port) - 5000
    self.alive = True

  @property
  def paxos(self):
    return self
  
  @property
  def ballot(self):
    reply = self.send({
      "type": "get_ballot"
    })
    if reply.get("status") == "error":
      return 0
    return reply.get("ballot", 0)
  
  def send(self, message):
    return self.node.send(self.address, message)
  
  def l_propose(self):
    print("ProxyNode called to lead proposal but not implemented :/")
    pass

  def f_receive_accept(self, metadata, ballot: int):
    reply = self.send({
      "type": "paxos_accept",
      "metadata": metadata._export(),
      "ballot": ballot,
    })
    print(f"  PAXOS Reply: {reply}")
    if reply.get("status") == "error":
      return Status.REJECT
    response_str = reply.get("response", "REJECT")
    try:
      return Status[response_str]
    except KeyError:
      return Status.REJECT
  
  def f_receive_commit(self, metadata, ballot: int):
    self.send({
      "type": "paxos_commit",
      "metadata": metadata._export(),
      "ballot": ballot,
    })