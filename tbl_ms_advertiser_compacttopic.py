
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

var_topic_src_name = 'NBC_APPS.TBL_MS_ADVERTISER'
var_topic_tgt_name = 'NBC_APPS.TBL_MS_ADVERTISER_COMPACT1'
var_cassandra_tgt_tab  = 'tbl_plan_advertiser'
var_cassandra_tgt_ks  = 'key1'

producer = KafkaProducer(bootstrap_servers='localhost:9092')

y = StructType([StructField("table1",StringType(), nullable = True),
                StructField("op_type",StringType(), nullable = True),
                StructField("op_ts",StringType(), nullable = True),
                StructField("current_ts",StringType(), nullable = True),
                StructField("pos",StringType(), nullable = True),
                StructField("ADVERTISER_ID",LongType(), nullable = False),
                StructField("ADVERTISER_NAME",StringType(), nullable = True),
                StructField("ADVERTISER_CREATION_DT",StringType(), nullable = True),
                StructField("ADVERTISER_MODIFICATION_DT",StringType(), nullable = True),
                StructField("CREATE_DATE",StringType(), nullable = True),
                StructField("ACCOUNTING_IDENT",StringType(), nullable = True)
                ])

#----------------------------------------
# 00003 - Get the Schema of Source Topic : 
#----------------------------------------

from schema import getting_value_schema,getting_key_schema
var_val_schema =getting_value_schema(var_cassandra_conn_host, var_topic_src_name,var_schema_url_port)
var_key_schema =getting_key_schema(var_cassandra_conn_host, var_topic_src_name,var_schema_url_port)


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
        var_adv_id = var_loaded_r['ADVERTISER_ID']
        var_op_type = var_loaded_r['op_type']
        print(var_adv_id)
        print(var_op_type)

        if var_val_value is not None:
           print ('thisisinsideparms')
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
                 .filter("ADVERTISER_ID =%d"%var_adv_id)
     
              ds.show()

              data = [record[1]]

              df=spark.sparkContext.parallelize(data).toDF(schema = y)   
              print("this is record dataframe ")
              df.show()
              inner_join = df.join(ds, df.ADVERTISER_ID == ds.ADVERTISER_ID).select(
                           ds.PLAN_ID,\
                           ds.op_type,\
                           ds.ADVERTISER_ID,\
                           df.ADVERTISER_NAME,\
                           ds.ORIGINAL_PLAN_ID,\
                           ds.PLAN_NAME,\
                           ds.PORTFOLIO_PLAN_ID,\
                           ds.PRG_PLAN_ID,\
                           ds.PROPERTY_ID,\
                           ds.REVISION_NO,\
                           ds.STEW_LINK_ID)

              inner_join.show()

              if inner_join.rdd.isEmpty():
                 print("No data available for this advertiser_id in cassandra table = %s"%var_adv_id)
              else:
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
