"""Write a consistent copy of the recorder database, safe while record.py is running.

Copying books.sqlite directly while it is being written can produce a broken file.

    python3 backup.py                    # writes data/books-copy.sqlite
    python3 backup.py path/to/copy.sqlite
"""
import os
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(HERE, "data", "books.sqlite")
dst_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "data", "books-copy.sqlite")

if os.path.exists(dst_path):
    os.remove(dst_path)
src = sqlite3.connect(f"file:{src_path}?mode=ro", uri=True)
dst = sqlite3.connect(dst_path)
src.backup(dst)
dst.execute("PRAGMA journal_mode=DELETE")  # one self-contained file, ready to download
cycles, books = dst.execute("SELECT (SELECT COUNT(*) FROM cycles), (SELECT COUNT(*) FROM books)").fetchone()
dst.close()
src.close()
print(f"Wrote {dst_path}: {cycles} cycles, {books} book rows, {os.path.getsize(dst_path) / 1e6:.1f} MB")
