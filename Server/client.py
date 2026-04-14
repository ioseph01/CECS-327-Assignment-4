import socket
import json
from config import SOCKET_TIMEOUT, BUFFER_SIZE


'''

Example metadata JSON:

{
 "filename": "music.json",
 "size_bytes": 40960,
 "num_pages": 3,
 "pages": [
     {"page_no": 0, "guid": "g1", "replicas": ["r1","r2","r3"]},
     {"page_no": 1, "guid": "g2", "replicas": ["r4","r5","r6"]},
     {"page_no": 2, "guid": "g3", "replicas": ["r7","r8","r9"]}
 ],
 "version": 12
}

'''

def send_message(host, port: int, message: dict):
  try:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
      sock.settimeout(SOCKET_TIMEOUT)
      sock.connect((host, port))

      data = json.dumps(message).encode("utf-8")
      # print(f"CLIENT: sending {data[:80]}")
      sock.sendall(data)
      sock.shutdown(socket.SHUT_WR)

      chunks = []
      while 1:
        chunk = sock.recv(BUFFER_SIZE)
        # print(f"CLIENT: got chunk {len(chunk)} bytes") 
        if not chunk or chunk is None:
          break
        chunks.append(chunk)

      raw = b"".join(chunks)
      # print(f"CLIENT: raw reply {raw[:80]}") 
      return json.loads(raw.decode("utf-8"))
    
  except Exception as e:
    return {"status": "error", "reason": str(e)}