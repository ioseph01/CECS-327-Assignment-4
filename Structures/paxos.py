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
    ''' Leader proposed a metadata update '''
    print(f"  PAXOS: Starting propose, ballot will be {self.ballot + 1}, replica ids={[r.id for r in replica_nodes]}")
    leader_id = min(replica.id for replica in replica_nodes if replica.alive)
    if self.id != leader_id or not self.node.alive:
      # Not the leader
      return False
    
    self.ballot += 1 + max(replica.paxos.ballot for replica in replica_nodes if replica.alive)
    ballot = self.ballot

    print(f"\n  PAXOS: Leader: node {self.id}")
    print(f"  PAXOS: Proposing ballot {ballot} for '{metadata.file_name}'")

    c = 0
    for replica in replica_nodes:
      if not replica.alive:
        continue
      print(f"  PAXOS: Sending ACCEPT ({ballot}) to node {replica.id}")
      response = replica.paxos.f_receive_accept(metadata, ballot)
      if response == Status.LEARN:
        print(f"  PAXOS: Received LEARN ({ballot}) from node {replica.id}")
        c += 1

    majority = len(replica_nodes) // 2 + 1
    if majority <= c:
      print(f"  PAXOS: Majority reached ({c}/{len(replica_nodes)}), committing")
      
      for replica in replica_nodes:
        if not replica.alive:
          continue
        replica.paxos.f_receive_commit(metadata, ballot)
      return True
    
    print(f"  PAXOS: Majority not reached ({c}/{len(replica_nodes)}), aborting")
    return False


  def f_receive_accept(self, metadata, ballot : int):
    if self.ballot <= ballot:
      self.ballot = ballot
      self.pending[ballot] = metadata
      return Status.LEARN
    # print(f"  PAXOS: Node {self.id} has already seen ballot {ballot}, rejecting.")
    print(f"  PAXOS: Node {self.id} rejecting ballot {ballot}, self.ballot={self.ballot}")

    return Status.REJECT

  

  def f_receive_commit(self, metadata, ballot : int):

    self.store[metadata.key] = metadata
    self.log.append(
      {
        "ballot": ballot,
        "file_name": metadata.file_name,
        "metadata": metadata._export()
      }
    )

    if ballot in self.pending:
      del self.pending[ballot]
    print(f"  PAXOS: Node {self.id} committing ballot {ballot} for {metadata.file_name}.")


  def print_log(self) -> None:
    print(f"\n  PAXOS: Log for node {self.id}:")
    if not self.log:
        print("    Empty Log")
    for log in self.log:
        print(f"    ballot={log['ballot']}, file={log['file_name']}")


    