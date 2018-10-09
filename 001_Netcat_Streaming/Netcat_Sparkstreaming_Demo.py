import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ =='__main__':
    # check if given system argument is 3
    if len(sys.argv) != 3:
        print("Usage spark-submit nccat_sparksubmit.py <hostname> <port>",file=sys.stderr)
        exit(-1)

    # read host and port numbers
    host = sys.argv[1]
    port = int(sys.argv[2])

    # initiate spark session
    # spark session is the entry point to spark environment
    # Sparkcontext, Hivecontext can be access using spark session
    spark = SparkSession\
            .builder\
            .appName("nccat_Demo_sparkstream")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    # set error level for this application
    # spark.sparkContext.setLoglevel("ERROR")

    
    # lines dataframe contains the text stream
    lines = spark\
            .readStream\
            .format('socket')\
            .option('host',host)\
            .option('port',port)\
            .load()

    # words dataframe - to split words from lines
    # put each word in a line
    words = lines.select(
        explode(
            split(lines.value," ")
        ).alias('word')
    )

    wordcount = words.groupBy('word').count()

    # data sink to console
    query = wordcount.writeStream\
                     .outputMode('complete')\
                     .format('console')\
                     .start()

    query.awaitTermination()