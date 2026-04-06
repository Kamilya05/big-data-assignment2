from pyspark.sql import SparkSession
from pathvalidate import sanitize_filename

spark = SparkSession.builder \
    .appName('data preparation') \
    .getOrCreate()

df = spark.read.parquet("a.parquet")

n = 100
df = df.select(['id', 'title', 'text']) \
       .sample(fraction=100 * n / df.count(), seed=0) \
       .limit(n)

def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])


df.foreach(create_doc)

rdd = df.rdd.map(lambda r: f"{r['id']}\t{r['title']}\t{r['text']}")

rdd = rdd.coalesce(1)

rdd.saveAsTextFile("/input/data")

spark.stop()