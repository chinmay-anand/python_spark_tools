############################################################
‘’’
Quick Example
REF: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#quick-example
        
Let’s say you want to maintain a running word count of text data received from a data server listening on a TCP socket. Let’s see how you can express this using Structured Streaming. You can see the full code in Scala/Java/Python/R. And if you download Spark, you can directly run the example. In any case, let’s walk through the example step-by-step and understand how it works. First, we have to import the necessary classes and create a local SparkSession, the starting point of all functionalities related to Spark.
‘’’

############################################################
#import the necessary classes and create a local SparkSession, the starting point of all functionalities related to Spark.
###---------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

###########################################
#Next, let’s create a streaming DataFrame that represents text data received from a server listening on localhost:9999, and transform the DataFrame to calculate word counts.   We have now set up the query on the streaming data.
###----------------------------------------

# Create DataFrame representing the stream of input lines from connection to localhost:9999
# Read text from socket with a socketDF (here ‘lines’)

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Returns True for DataFrames that have streaming sources
# socketDF.isStreaming()  ### or ### line.isStreaming()

# Split the lines into words
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()

###########################################
# Start receiving data and computing the counts
###----------------------------------------

 # Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

###########################################
# Run Netcat (a small utility found in most Unix-like systems) as a data server by using
$ nc -lk 9999

###########################################
# Then, in a different terminal, you can start the example by using
###----------------------------------------
$ ./bin/spark-submit examples/src/main/python/sql/streaming/structured_network_wordcount.py localhost 9999
########################################################
########################################################
### ANOTHER EXAMPLE
###----------------
### 
########################################################
spark = SparkSession. ...

# Read text from socket
socketDF = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

socketDF.isStreaming()    # Returns True for DataFrames that have streaming sources

socketDF.printSchema()

# Read all the csv files written atomically in a directory
userSchema = StructType().add("name", "string").add("age", "integer")
csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("/path/to/directory")
# Equivalent to format("csv").load("/path/to/directory")
