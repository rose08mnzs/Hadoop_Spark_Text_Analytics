#!/usr/bin/env python3
import sys
from collections import defaultdict
c = defaultdict(int)
for line in sys.stdin:
    w, v = line.strip().split('\t')
    c[w] += int(v)
for w, cnt in c.items():
    print(f"{w}\t{cnt}")
