#!/usr/bin/env python3
import subprocess
import sys
from cassandra.cluster import Cluster

def main():
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS search_engine
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace('search_engine')
    session.execute("""
        CREATE TABLE IF NOT EXISTS inverted_index (
            term text,
            doc_id text,
            tf int,
            PRIMARY KEY (term, doc_id)
        )
    """)
    print("Schema ready")
    insert_stmt = session.prepare(
        "INSERT INTO inverted_index (term, doc_id, tf) VALUES (?, ?, ?)"
    )

    proc = subprocess.Popen(
        ['hdfs', 'dfs', '-cat', '/indexer/index/part-*'],
        stdout=subprocess.PIPE,
        text=True
    )

    count = 0
    for line in proc.stdout:
        line = line.strip()
        if not line:
            continue
        parts = line.split('\t')
        if len(parts) < 3:
            continue
        term, doc_id, tf_str = parts[0], parts[1], parts[2]
        session.execute(insert_stmt, (term, doc_id, int(tf_str)))
        count += 1
        if count % 1000 == 0:
            print(f"Inserted {count} rows")

    proc.wait()
    if proc.returncode != 0:
        print("HDFS read error")
        sys.exit(1)

    print(f"Done. Total {count} rows inserted.")
    cluster.shutdown()

if __name__ == "__main__":
    main()