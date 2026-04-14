import socket
import json
import threading
from config import HOST, BUFFER_SIZE


class Server:
  def __init__(self, node, port: int):
    self.node = node
    self.host = HOST
    self.port = port
    self.running = False


  def start(self):
    self.running = True

    thread = threading.Thread(target=self.listen, daemon=True)
    thread.start()
    print(f"SERVER: Node {self.node.id} listening on {self.host}:{self.port}")

  def listen(self):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
      sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      sock.bind((self.host, self.port))
      sock.listen()
      while self.running:
        try:
          connection, address = sock.accept()
          thread = threading.Thread(
            target=self.handle,
            args=(connection,),
            daemon=True,
          )
          thread.start()
        except Exception as e:
          if self.running:
            print(e)

  def handle(self, connection):
    try:
      chunks = []
      while 1:
        chunk = connection.recv(BUFFER_SIZE)
        if not chunk or chunk is None:
          break
        chunks.append(chunk)

      raw = b"".join(chunks)
      print(f"SERVER: received {raw[:80]}")
      message = json.loads(raw.decode("utf-8"))
      reply = self.dispatch(message)
      print(f"SERVER: replying {str(reply)[:80]}")
      connection.sendall(json.dumps(reply).encode("utf-8"))
    except Exception as e:
      try:
          connection.sendall(json.dumps({
            "status": "Error",
            "reason": str(e),
          }).encode("utf-8"))
      except:
          pass
    finally:
      connection.close()



  def dispatch(self, message: dict) -> dict:
 
    msg_type = message.get("type")

    ### Chord ###
    if msg_type == "find_succ":
      return self.node.handle_find_succ(message)

    elif msg_type == "get_succ":
      return self.node.handle_get_succ(message)

    elif msg_type == "get_pred":
      return self.node.handle_get_pred(message)

    elif msg_type == "notify":
      return self.node.handle_notify(message)

    elif msg_type == "put":
      return self.node.handle_put(message)

    elif msg_type == "get":
      return self.node.handle_get(message)

    elif msg_type == "delete":
      return self.node.handle_delete(message)
    
    ### DFS ###
    elif msg_type == "dfs_touch":
      return self.node.handle_dfs_touch(message)

    elif msg_type == "dfs_append":
      return self.node.handle_dfs_append(message)

    elif msg_type == "dfs_read":
      return self.node.handle_dfs_read(message)

    elif msg_type == "dfs_head":
      return self.node.handle_dfs_head(message)

    elif msg_type == "dfs_tail":
      return self.node.handle_dfs_tail(message)

    elif msg_type == "dfs_delete":
      return self.node.handle_dfs_delete(message)

    elif msg_type == "dfs_ls":
      return self.node.handle_dfs_ls(message)

    elif msg_type == "dfs_stat":
      return self.node.handle_dfs_stat(message)

    elif msg_type == "dfs_sort":
      return self.node.handle_dfs_sort(message)

    ### Sort ###
    elif msg_type == "sort_route":
        return self.node.handle_sort_route(message)

    elif msg_type == "sort_collect":
        return self.node.handle_sort_collect(message)


    ### Paxos ###
    elif msg_type == "paxos_accept":
        return self.node.handle_paxos_accept(message)

    elif msg_type == "paxos_commit":
      return self.node.handle_paxos_commit(message)
    
    elif msg_type == "get_ballot":
      return self.node.handle_get_ballot(message)
    
    return {"status": "error", "reason": f"unknown message type: {msg_type}"}


  def stop(self):
      self.running = False