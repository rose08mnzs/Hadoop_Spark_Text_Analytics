#!/usr/bin/env python3
import re
import sys

WORD_RE = re.compile(r"[A-Za-z]+'?[A-Za-z]+|[A-Za-z]+")

for line in sys.stdin:
    for word in WORD_RE.findall(line.lower()):
        print(f"{word}\t1")
