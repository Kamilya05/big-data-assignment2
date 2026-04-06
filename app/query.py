import sys
import math
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

query = sys.argv[1].lower().split()

spark = SparkSession.builder.appName("Search").getOrCreate()

cluster = Cluster(['cassandra-server'])
session = cluster.connect('search_engine')

rows = session.execute("SELECT term, doc_id, tf FROM inverted_index")

data = [(r.term, r.doc_id, r.tf) for r in rows]
rdd = spark.sparkContext.parallelize(data)

N = len(set([d[1] for d in data]))

def compute_scores(doc):
    doc_id = doc[0]
    terms = doc[1]

    score = 0
    for term in query:
        for t, _, tf in terms:
            if t == term:
                df = len([x for x in data if x[0] == term])
                idf = math.log((N + 1) / (df + 1))

                score += tf * idf

    return (doc_id, score)

import math

def compute_bm25(doc, avgdl, k1=1.5, b=0.75):
    doc_id = doc[0]
    terms = doc[1]
    doc_len = sum(tf for _, _, tf in terms)
    score = 0.0
    for q_term in query:
        tf = 0
        for term, _, t_f in terms:
            if term == q_term:
                tf = t_f
                break
        if tf == 0:
            continue

        df = len([x for x in data if x[0] == q_term])
        idf = math.log((N - df + 0.5) / (df + 0.5))
        numerator = tf * (k1 + 1)
        denominator = tf + k1 * (1 - b + b * (doc_len / avgdl))
        score += idf * (numerator / denominator)

    return (doc_id, score)

total_len = rdd.map(lambda x: x[1]) \
    .distinct() \
    .map(lambda doc_id: sum(tf for term, d, tf in data if d == doc_id)).sum()

num_docs = rdd.map(lambda x: x[1]).distinct().count()
avgdl = total_len / num_docs if num_docs else 1.0

doc_rdd = rdd.groupBy(lambda x: x[1]) \
    .map(lambda x: (x[0], list(x[1]))) \
    .map(compute_scores)

top_docs = doc_rdd.sortBy(lambda x: -x[1]).take(10)

def parse_line(line):
    parts = line.split("\t")
    if len(parts) >= 2:
        return (parts[0], parts[1])
    else:
        return None

top_docs_rdd = spark.sparkContext.parallelize(top_docs)

title_rdd = spark.sparkContext.textFile("/input/data/part-00000") \
    .map(parse_line).filter(lambda x: x is not None)

print(F"Search resu;ts from {N} indexed documents.")

joined = top_docs_rdd.join(title_rdd)
result = joined.collect()
print("Top documents TF-IDF:")
for doc_id, (score, title) in sorted(result, key=lambda x: x[1][0], reverse=True):
    print(f"{doc_id}, {title}, {score}")

doc_rdd = rdd.groupBy(lambda x: x[1]) \
    .map(lambda x: (x[0], list(x[1]))) \
    .map(lambda x: compute_bm25(x, avgdl))

top_docs = doc_rdd.sortBy(lambda x: -x[1]).take(10)

top_docs_rdd = spark.sparkContext.parallelize(top_docs)

joined = top_docs_rdd.join(title_rdd)
result = joined.collect()
print("Top documents BM25:")
for doc_id, (score, title) in sorted(result, key=lambda x: x[1][0], reverse=True):
    print(f"{doc_id}, {title}, {score}")