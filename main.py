# test_chord_dfs.py
from Structures.chord import Chord, Node
from api import DFS

def test_sort(dfs):
    print("\n=== Sort Test ===")

    # create a DFS file with 10 key,value records out of order
    records = [
        "0042,bob",
        "0012,alice",
        "0190,carol",
        "0031,dave",
        "0055,eve",
        "0008,frank",
        "0100,grace",
        "0077,heidi",
        "0003,ivan",
        "0200,judy",
    ]

    # write records to a local file and append into DFS
    with open("sort_input.txt", "w") as f:
        f.write("\n".join(records))

    dfs.touch("input.csv")
    print(dfs.append("input.csv", "sort_input.txt"))

    # sort
    print(dfs.sort_file("input.csv", "output.csv"))

    # debug
    meta = dfs.get_metadata("output.csv")
    print(f"output.csv metadata: {meta._export() if meta else None}")

    for pk in (meta.page_keys if meta else []):
        page = dfs.get_page(pk)
        print(f"  page_key={pk} -> {page}")

    # verify
    print("\nsorted output:")
    print(dfs.read("output.csv"))



def make_ring():
    chord = Chord()
    for node_id in [4, 8, 15, 27, 44, 58]:
        node = Node(chord, node_id)
        chord.add_node(node)
    return chord

def test_ring(chord):
    print("=== Ring Structure ===")
    chord.print_ring()
    print()
    chord.print_finger_tables()

def test_dfs(dfs):
    print("=== DFS Operations ===")

    # touch
    print(dfs.touch("music.txt"))
    print(dfs.touch("music.txt"))  # should say already exists

    # stat on empty file
    print("\nstat after touch:")
    print(dfs.stat("music.txt"))

    # create a small local file to append
    with open("test_input.txt", "w") as f:
        f.write("hello world\n" * 100)

    # append
    print("\n" + dfs.append("music.txt", "test_input.txt"))
    print("\nstat after append:")
    print(dfs.stat("music.txt"))

    # read
    print("\nread (first 50 chars):")
    print(dfs.read("music.txt")[:50])

    # head
    print("\nhead 3:")
    print(dfs.head("music.txt", 3))

    # tail
    print("\ntail 3:")
    print(dfs.tail("music.txt", 3))

    # ls
    print("\nls:")
    print(dfs.ls())

    # touch a second file
    dfs.touch("notes.txt")
    print("\nls after second touch:")
    print(dfs.ls())

    # delete
    print("\n" + dfs.delete_file("music.txt"))
    print("\nls after delete:")
    print(dfs.ls())

    # read deleted file
    print("\nread after delete:")
    print(dfs.read("music.txt"))

if __name__ == "__main__":
    chord = make_ring()
    test_ring(chord)
    entry = chord.nodes[4]
    dfs = DFS(chord, entry)
    test_dfs(dfs)
    test_sort(dfs)