#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.rstrip("\n")  # drop newline only
    for ch in line:
        # escape control chars so delimiter safety holds
        esc = ch.encode('unicode_escape').decode('ascii')
        print(f"{esc}\t1")
