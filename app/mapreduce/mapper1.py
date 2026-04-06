#!/usr/bin/env python3
import sys
import re

for line in sys.stdin:
    try:
        parts = line.strip().split("\t")

        if len(parts) < 2:
            continue

        doc_id = parts[0]
        text = " ".join(parts[1:])   # ВСЁ остальное считаем текстом

        words = re.findall(r'\w+', text.lower())

        for word in words:
            print(f"{word}\t{doc_id}")

    except Exception as e:
        continue