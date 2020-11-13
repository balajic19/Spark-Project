import findspark
findspark.init('C:\\Spark')

from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('Spark Structed streaming with kafka demo').getOrCreate()


KAFKA_BOOTSTRAP_CONS = 'localhost:9092'
KAFKA_TOPIC = 'sample_topic'
df = spark.readStream\
    .format('kafka')\
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_CONS)\
            .option('subscribe', KAFKA_TOPIC)\
                .option('startingOffsets', 'latest')


print(df.show())
print(spark)
print('hello world')
spark.stop()