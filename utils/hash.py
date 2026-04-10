import hashlib
from config import M

def hash_key(key: str) -> int:
    digest = hashlib.sha1(key.encode()).hexdigest()
    return int(digest, 16) % (2 ** M)


