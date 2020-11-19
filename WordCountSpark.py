import io
import string
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

  conf = SparkConf().setAppName("Spark Word Count")
  sc = SparkContext(conf=conf)
  

  LineData = sc.textFile("hamlet.txt").flatMap(lambda line: line.split(" "))
  
  
  wordFrequencies = LineData.map(lambda word: (word.translate(str.maketrans('', '', string.punctuation)).lower(), 1)).reduceByKey(lambda v1,v2:v1 +v2)

  res = wordFrequencies.collect()
  
  with io.open("hamletout.txt",'w',encoding="utf-8") as f:
    f.write(str(res))
   

  
