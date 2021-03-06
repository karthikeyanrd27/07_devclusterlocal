from pyspark import SparkConf, SparkContext
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pyspark.sql import SQLContext, SparkSession

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
from pyspark.streaming.kafka import KafkaUtils, OffsetRange, TopicAndPartition
import avro.schema
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from variables import *
from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro


#------------------------------
# 00000 - Varible Declaration : 
#------------------------------

var_topic_src_name = 'NBC_APPS.TBL_MS_PROPERTY'
var_topic_tgt_name = 'NBC_APPS.TBL_MS_PROPERTY_COMPACT1'

producer = KafkaProducer(bootstrap_servers='localhost:9092')


y = StructType([StructField("table1",StringType(), nullable = True),
                StructField("op_type",StringType(), nullable = True),
                StructField("op_ts",StringType(), nullable = True),
                StructField("current_ts",StringType(), nullable = True),
                StructField("pos",StringType(), nullable = True),
                StructField("PROPERTY_ID",LongType(), nullable = False),
                StructField("PROPERTY_NAME",StringType(), nullable = True),
                StructField("NETWORK_ID",StringType(), nullable = True),
                StructField("PARENT_CHANNEL_ID",StringType(), nullable = True),
                StructField("AUTOMATION_CODE",StringType(), nullable = True),
                StructField("AUTOMATION_NAME",StringType(), nullable = True),
                StructField("CALL_SIGN_NAME",StringType(), nullable = True),
                StructField("SALES_DAYPART_SET_ID",StringType(), nullable = True),
                StructField("CREATE_DATE",StringType(), nullable = True),
                StructField("RATED_NETWORK",StringType(), nullable = True)
                ])

#----------------------------------------
# 00003 - Get the Schema of Source Topic : 
#----------------------------------------

from schema import getting_value_schema,getting_key_schema
var_val_schema =getting_value_schema(var_cassandra_conn_host, var_topic_src_name,var_schema_url_port)
var_key_schema =getting_key_schema(var_cassandra_conn_host, var_topic_src_name,var_schema_url_port)
print(var_val_schema)
print(var_key_schema)
print(type(var_val_schema))


value_schema = avro.loads(var_val_schema)
key_schema = avro.loads(var_key_schema)


from df import getting_df_value_schema
var_df_schema = getting_df_value_schema(var_val_schema)



#--------------------------------------------
# 00004 - Processing the Each Kafka Messages : 
#--------------------------------------------

# This part of Code writing the messages into compact topic :

def handler(message):
    records = message.collect()
    for record in records:
        var_val_key = record[0]
        var_val_value = record[1]
        print(type(var_val_key))
        print(type(var_val_value))
        var_json_dum = json.dumps(var_val_value)
        var_loaded_r = json.loads(var_json_dum)
        var_property_id = var_loaded_r['PROPERTY_ID']
        var_op_type = var_loaded_r['op_type']
        print(var_property_id)
        print(var_op_type)

        if var_val_value is not None:
           print ('thisisinside')
           print(value_schema)
           print(key_schema)
           print('thisisschemaavrowriting')
           var_kafka_parms_tgt = {'bootstrap.servers': var_bootstrap_servr,'schema.registry.url': var_schema_url} 
           avroProducer = AvroProducer(var_kafka_parms_tgt,default_key_schema=key_schema, default_value_schema=value_schema)
           avroProducer.produce(topic=var_topic_tgt_name, value=var_val_value, key=var_val_key)
           avroProducer.flush()

# This part to Updated the Target Cassandra Table based on the updated records in reference table:

           if var_op_type =='U':
              print ('karthikeyanbanaveenthan')
              sqlContext = SQLContext(spark)
              ds = sqlContext \
                 .read \
                 .format('org.apache.spark.sql.cassandra') \
                 .options(table=var_cassandra_tgt_tble, keyspace=var_cassandra_tgt_ks) \
                 .load() \
                 .filter("PROPERTY_ID =%d"%var_property_id)
              print("this is ds dataframe")     
              ds.show()

              data = [record[1]]

              df=spark.sparkContext.parallelize(data).toDF(schema = y)   
              print("this is record dataframe ")
              df.show()

              inner_join = df.join(ds, df.PROPERTY_ID == ds.PROPERTY_ID).select(
                           ds.PLAN_ID,\
                           ds.op_type,\
                           ds.PROPERTY_ID,\
                           df.PROPERTY_NAME)

              inner_join.show()
 
              if inner_join.rdd.isEmpty():
                 print("No data available for this property_id in cassandra table = %s"%var_property_id)
              else:
                 print("cassandrawriting")
                 inner_join.write\
                      .format("org.apache.spark.sql.cassandra")\
                      .mode('append')\
                      .options(table=var_cassandra_tgt_tble, keyspace=var_cassandra_tgt_ks)\
                      .save() 

#----------------------------------------
# 00001 - Spark Configuration Declaration : 
#----------------------------------------

schema_registry_client = CachedSchemaRegistryClient(var_schema_url)
serializer = MessageSerializer(schema_registry_client)

spark = SparkSession.builder \
  .appName('SparkCassandraApp') \
  .config('spark.cassandra.connection.host' ,var_cassandra_conn_host) \
  .config('spark.cassandra.connection.port',var_cassandra_conn_port) \
  .config('spark.cassandra.output.consistency.level',var_cassandra_cons_level) \
  .master('local[2]') \
  .getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc, var_streaming_duration)


#------------------------------------------
# 00002 - Spark Streaming from Source Topic : 
#------------------------------------------

kvs = KafkaUtils.createDirectStream(ssc, [var_topic_src_name], var_kafka_parms_src,valueDecoder=serializer.decode_message)
kvs.foreachRDD(handler)

ssc.start()
ssc.awaitTermination()
