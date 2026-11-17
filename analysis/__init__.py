"""Analysis of the recorded Kalshi and Polymarket data.

Vihaan's original modules (edge.py, maker.py, analyze_*.py) import each other as top-level modules
(`from edge import ...`) and are kept unchanged, so this package's folder is added to the import path.
The project root is added too, for the shared `recorder.*` code. Both are appended, so nothing shadows
the standard library.
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (_HERE, os.path.dirname(_HERE)):
    if _p not in sys.path:
        sys.path.append(_p)
