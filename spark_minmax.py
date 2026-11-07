from pyspark import SparkContext
import re
sc = SparkContext("local[*]", "MinMax")

WORD_RE = re.compile(r"[A-Za-z]+'?[A-Za-z]+|[A-Za-z]+")
text = sc.textFile("hdfs:///gutenberg/*.txt")

counts = (text.flatMap(lambda line: WORD_RE.findall(line.lower()))
              .map(lambda w: (w, 1))
              .reduceByKey(lambda a, b: a + b))

min_word = counts.min(key=lambda x: x[1])
max_word = counts.max(key=lambda x: x[1])

print("MIN:", min_word)
print("MAX:", max_word)
