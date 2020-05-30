############################################################
‘’’
Quick Example
REF: https://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example

We want to count the number of words in text data received from a data server listening on a TCP socket.
‘’’

###########################################
#We create a local StreamingContext with two execution threads, and batch interval of 1 second.
###----------------------------------------

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")   ## SYNTAX: SparkContext(master, appName)
ssc = StreamingContext(sc, 1)
###----------------------------------------
# This “lines” DStream represents the stream of data that will be received from the data server. Each record in this DStream is a line of text. Next, we want to split the lines by space into words.
###----------------------------------------
# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

###----------------------------------------
#A Discretized Stream (DStream), the basic abstraction in Spark Streaming,
# is a continuous sequence of RDDs (of the same type) representing a continuous stream of data 
#flatMap is a one-to-many DStream operation that creates a new DStream by generating multiple new records 
# from each record in the source DStream. In this case, each line will be split into multiple words.
###---------------------------------------

words = lines.flatMap(lambda line: line.split(" "))

###---------------------------------------
### The words DStream is further mapped (one-to-one transformation) to a DStream of (word, 1) pairs, which is then reduced to get the frequency of words in each batch of data. Finally, wordCounts.pprint() will print a few of the counts generated every second.

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()
###----------------------------------------

###########################################
### To start the processing after all the transformations have been setup, we finally call
###----------------------------------------
ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

###########################################
# Run Netcat (a small utility found in most Unix-like systems) as a data server by using
$ nc -lk 9999

###########################################
# Then, in a different terminal, you can start the example by using
###----------------------------------------
$ ./bin/spark-submit examples/src/main/python/streaming/network_wordcount.py localhost 9999
########################################################

