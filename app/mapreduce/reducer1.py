#!/usr/bin/env python3
import sys
from collections import defaultdict

current_word = None
doc_counts = defaultdict(int)

for line in sys.stdin:
    word, doc_id = line.strip().split("\t")

    if current_word and word != current_word:
        for d, count in doc_counts.items():
            print(f"{current_word}\t{d}\t{count}")
        doc_counts = defaultdict(int)

    current_word = word
    doc_counts[doc_id] += 1

# last word
if current_word:
    for d, count in doc_counts.items():
        print(f"{current_word}\t{d}\t{count}")