#!/usr/bin/env python3
import sys
from collections import defaultdict

counts = defaultdict(int)
for raw in sys.stdin:
    line = raw.rstrip("\n")
    parts = line.split('\t')
    if len(parts) != 2:
        continue
    k, v = parts
    try:
        counts[k] += int(v)
    except ValueError:
        continue

for k, cnt in counts.items():
    print(f"{k}\t{cnt}")
