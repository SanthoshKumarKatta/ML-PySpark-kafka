import findspark
findspark.init()
from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import pyspark
KAFKA_TOPIC = "trump"
KAFKA_BROKERS = "localhost:9092"
ZOOKEEPER = "localhost:2181"
print(1)
conf = pyspark.SparkConf().set('spark.driver.host','127.0.0.1')
sc = pyspark.SparkContext(master='local[*]', appName='PythonStreamingKafkaWordCount',conf=conf)
ssc = StreamingContext(sc, 1)

print(2)
kafkaStream = KafkaUtils.createStream(ssc, ZOOKEEPER, "spark-streaming123", {KAFKA_TOPIC:1})

print(3)
lines = kafkaStream.map(lambda x: x[1])
lines.pprint()

ssc.start()
ssc.awaitTermination()